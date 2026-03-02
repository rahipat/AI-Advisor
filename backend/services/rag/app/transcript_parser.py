import logging
import os
import re
from dataclasses import dataclass, asdict

try:
    from supabase import create_client, Client
except ImportError:  # pragma: no cover
    create_client = None
    Client = object

logger = logging.getLogger(__name__)

TERM_RE = re.compile(r"\b(Spring|Summer\s*(?:A|B|C)?|Fall)\s+(20\d{2})\b", re.IGNORECASE)
COURSE_CODE_RE = re.compile(r"\b([A-Z]{3}[0-9]{4}[A-Z]?)\b")
VALID_GRADES = {"A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "E", "F", "P", "S", "U", "W", "WF", "I", "NG"}
TRAILING_CREDIT_GRADE_RE = re.compile(
    r"\s+\d+(?:\.\d+)?\s+(A\+|A|A-|B\+|B|B-|C\+|C|C-|D\+|D|D-|E|F|P|S|U|W|WF|I|NG)$",
    re.IGNORECASE,
)
TRAILING_GRADE_RE = re.compile(
    r"\s+(A\+|A|A-|B\+|B|B-|C\+|C|C-|D\+|D|D-|E|F|P|S|U|W|WF|I|NG)$",
    re.IGNORECASE,
)


@dataclass
class TranscriptCourse:
    transcript_id: str
    user_id: str | None
    course_code: str
    course_title: str
    term: str | None
    grade: str | None


class TranscriptParserService:
    def __init__(
        self,
        supabase_url: str | None = None,
        supabase_key: str | None = None,
        source_table: str | None = None,
        source_text_column: str | None = None,
        destination_table: str = "transcript_courses",
    ):
        url = supabase_url or os.environ.get("SUPABASE_URL")
        key = supabase_key or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

        if create_client is None:
            raise ImportError("supabase package is required. Install dependencies from requirements.txt")

        self.source_table = source_table or os.environ.get("TRANSCRIPT_SOURCE_TABLE", "transcripts")
        self.source_text_column = source_text_column or os.environ.get("TRANSCRIPT_SOURCE_TEXT_COLUMN", "raw_text")
        self.destination_table = destination_table
        self.client: Client = create_client(url, key)

    def parse_transcript_text(self, transcript_id: str, raw_text: str, user_id: str | None = None) -> list[TranscriptCourse]:
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        parsed_courses: list[TranscriptCourse] = []

        last_term: str | None = None

        for idx, line in enumerate(lines):
            term_match = TERM_RE.search(line)
            if term_match:
                last_term = f"{term_match.group(1).title()} {term_match.group(2)}"

            code_match = COURSE_CODE_RE.search(line)
            if not code_match:
                continue

            course_code = code_match.group(1).upper()
            current_term = TranscriptParserService._resolve_term(lines, idx, fallback=last_term)
            grade = TranscriptParserService._extract_grade(line)
            title = TranscriptParserService._extract_course_title(lines, idx, course_code, grade)

            if not title:
                title = "Unknown Title"

            parsed_courses.append(
                TranscriptCourse(
                    transcript_id=transcript_id,
                    user_id=user_id,
                    course_code=course_code,
                    course_title=title,
                    term=current_term,
                    grade=grade,
                )
            )

        deduped: list[TranscriptCourse] = []
        seen_keys: set[tuple[str, str, str | None, str | None]] = set()
        for row in parsed_courses:
            key = (row.course_code, row.course_title, row.term, row.grade)
            if key not in seen_keys:
                seen_keys.add(key)
                deduped.append(row)

        return deduped

    def process_transcripts(self, limit: int = 200) -> int:
        query = (
            self.client.table(self.source_table)
            .select(f"id,user_id,{self.source_text_column}")
            .not_.is_(self.source_text_column, "null")
            .limit(limit)
        )
        response = query.execute()
        transcripts = response.data or []

        total_rows = 0
        for transcript in transcripts:
            transcript_id = transcript.get("id")
            user_id = transcript.get("user_id")
            raw_text = transcript.get(self.source_text_column)

            if not transcript_id or not raw_text:
                continue

            parsed_rows = self.parse_transcript_text(
                transcript_id=str(transcript_id),
                raw_text=raw_text,
                user_id=str(user_id) if user_id else None,
            )

            if not parsed_rows:
                logger.info("No transcript courses parsed for transcript_id=%s", transcript_id)
                continue

            self.client.table(self.destination_table).delete().eq("transcript_id", transcript_id).execute()
            payload = [asdict(row) for row in parsed_rows]
            self.client.table(self.destination_table).insert(payload).execute()
            total_rows += len(parsed_rows)

            logger.info("Parsed %s courses for transcript_id=%s", len(parsed_rows), transcript_id)

        return total_rows

    @staticmethod
    def _resolve_term(lines: list[str], line_index: int, fallback: str | None) -> str | None:
        current = lines[line_index]
        term_match = TERM_RE.search(current)
        if term_match:
            return f"{term_match.group(1).title()} {term_match.group(2)}"

        lookback = max(0, line_index - 8)
        for i in range(line_index - 1, lookback - 1, -1):
            m = TERM_RE.search(lines[i])
            if m:
                return f"{m.group(1).title()} {m.group(2)}"

        return fallback

    @staticmethod
    def _extract_grade(line: str) -> str | None:
        tokens = [token.strip(",;()[]") for token in line.split()]
        for token in reversed(tokens):
            candidate = token.upper()
            if candidate in VALID_GRADES:
                return candidate
        return None

    @staticmethod
    def _extract_course_title(lines: list[str], line_index: int, course_code: str, grade: str | None) -> str:
        line = lines[line_index]
        after_code = line.split(course_code, 1)[-1].strip()

        if not after_code and line_index + 1 < len(lines):
            next_line = lines[line_index + 1]
            if not COURSE_CODE_RE.search(next_line) and not TERM_RE.search(next_line):
                after_code = next_line

        title = after_code

        title = TRAILING_CREDIT_GRADE_RE.sub("", title)
        title = TRAILING_GRADE_RE.sub("", title)

        if grade:
            title = re.sub(rf"\b{re.escape(grade)}\b$", "", title, flags=re.IGNORECASE).strip()

        title = re.sub(r"\s{2,}", " ", title).strip(" -:;")
        return title



def parse_transcript_text(raw_text: str, transcript_id: str = "", user_id: str | None = None) -> list[TranscriptCourse]:
    """Parse transcript raw text without requiring a Supabase client."""
    return TranscriptParserService.parse_transcript_text(None, transcript_id=transcript_id, raw_text=raw_text, user_id=user_id)

def run_transcript_parser(limit: int = 200) -> int:
    service = TranscriptParserService()
    return service.process_transcripts(limit=limit)


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    parsed = run_transcript_parser(limit=int(os.environ.get("TRANSCRIPT_PARSE_LIMIT", "200")))
    logger.info("Inserted %s transcript course rows", parsed)
