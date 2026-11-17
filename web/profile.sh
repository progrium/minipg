# The web demo's login shell: /root, with minipg on PATH.
export PSQL_PAGER= PS1='\[\e[1;34m\]\w\[\e[0m\] \$ '
cd /root
cat <<'MSG'
minipg: PostgreSQL as a single executable. This is minipg with Postgres 18 for 386
Linux, running in your browser under h9.

Try:
  minipg app.pgdb # setup db, start shell
  minipg app.pgdb "create table notes (id serial primary key, body text)"
  minipg app.pgdb "insert into notes (body) values ('hello')"
  minipg app.pgdb "select * from notes"
  minipg          # see all commands

MSG
