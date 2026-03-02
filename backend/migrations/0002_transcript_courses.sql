create table if not exists transcript_courses (
  id uuid primary key default gen_random_uuid(),
  transcript_id uuid not null,
  user_id uuid,
  course_code text not null,
  course_title text not null,
  term text,
  grade text,
  created_at timestamptz not null default now()
);

create index if not exists idx_transcript_courses_transcript_id on transcript_courses(transcript_id);
create index if not exists idx_transcript_courses_user_id on transcript_courses(user_id);
create index if not exists idx_transcript_courses_course_code on transcript_courses(course_code);

create unique index if not exists uniq_transcript_course_row
  on transcript_courses(transcript_id, course_code, course_title, coalesce(term, ''), coalesce(grade, ''));
