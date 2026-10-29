-- pg_cron_mp_sync_dispatch.sql
-- The marketplace sync (.github/workflows/marketplace-sync.yml) is STARTED by the database's clock (pg_cron + pg_net),
-- not by GitHub's. Same pattern as bc-supabase-sync/o2c/pg_cron_bc_sync_dispatch.sql.
--
-- WHY. The workflow's own schedule ("30 5,9,12 UTC" = 11:00 / 15:00 / 18:00 IST) was honoured 4-6 hours late: over
-- 24-28 Oct 2026 the first run of the day started between 15:13 and 16:59 IST, and on 29 Oct it had not started by
-- 13:16 IST. Harshil uploads yesterday's sell-out files at ~10:00 IST, so they reached mp_sales around 17:00 IST,
-- AFTER Birbal's once-daily 13:30 IST sales-facts refresh (Vercel cron sales-refresh), and the Sales / 360 boards
-- showed sell-out two days behind. A workflow_dispatch starts in seconds. Keyur, 2026-10-29: "Dispatch the
-- marketplace sync from pg_cron via pg_net every 4 hours".
--
-- WHEN (IST): 03:00, 07:00, 11:00, 15:00, 19:00, 23:00 -- every 4 hours, anchored so that the 11:00 run follows the
-- morning upload and lands (7-15 min, ~45 min when a month of files is new) before the 13:30 facts refresh. The
-- 15:00 IST tick also asks the workflow to send the data-gaps mail (input send_gaps_mail), which the workflow's own
-- 09:30 UTC schedule used to decide. No busy guard is needed: the workflow's concurrency group (marketplace-sync)
-- queues a second run behind a running one instead of overlapping it.
--
-- TOKEN. The same vault secret as the BC sync, 'github_bc_sync_dispatch': keyur-tbd's fine-grained token
-- "supabase-pg-cron-dispatch" with Actions: Read and write on SELECTED repositories -- that list must include
-- marketplace-ads-pipeline (besides bc-supabase-sync and pod-tracker) or GitHub answers 404 "Not Found" to the
-- dispatch. Rotate with
--   select vault.update_secret((select id from vault.secrets where name = 'github_bc_sync_dispatch'), '<token>');
-- An expired or revoked token shows as http_status 401/403 in public.mp_sync_dispatch_log; the workflow keeps ONE
-- GitHub schedule a day (12:30 UTC) as a fallback so the sync never stops altogether.
--
-- Apply as postgres, whole file, idempotent (hand-applied; it is not part of mp.schema / --init-schema).
-- Check with:
--   select * from public.mp_sync_dispatch_log order by id desc limit 10;
--   select * from cron.job_run_details where jobid = (select jobid from cron.job where jobname = 'mp-sync-dispatch') order by start_time desc limit 5;

create extension if not exists pg_net with schema extensions;

create table if not exists public.mp_sync_dispatch_log (
  id             bigserial primary key,
  requested_at   timestamptz not null default now(),
  send_gaps_mail boolean,
  skipped_reason text,
  net_request_id bigint,
  http_status    integer,
  response       text
);
alter table public.mp_sync_dispatch_log enable row level security;
revoke all on public.mp_sync_dispatch_log from anon, authenticated;
comment on table public.mp_sync_dispatch_log is
  'One row per pg_cron tick of mp-sync-dispatch (pg_cron_mp_sync_dispatch.sql): the GitHub workflow_dispatch request for '
  'marketplace-sync.yml, or why it was skipped. http_status 204 = GitHub accepted it; 401/403 = the vault token is wrong '
  'or expired; 404 = the token''s repository list does not include marketplace-ads-pipeline. send_gaps_mail = the 15:00 '
  'IST tick, which also sends the data-gaps mail.';

-- p_send_gaps_mail: null = decide by the clock (true only at 15:00 IST); pass false for a test fire.
create or replace function public.mp_sync_dispatch(p_send_gaps_mail boolean default null)
  returns jsonb
  language plpgsql
  security definer
  set search_path = pg_catalog, public, pg_temp
as $function$
declare
  v_token text;
  v_id    bigint;
  v_mail  boolean;
begin
  -- earlier requests: pg_net answers asynchronously and keeps responses for 6 hours
  update public.mp_sync_dispatch_log l
     set http_status = r.status_code,
         response = left(coalesce(nullif(r.content, ''), r.error_msg, case when r.timed_out then 'timed out' end), 500)
    from net._http_response r
   where r.id = l.net_request_id and l.http_status is null;

  v_mail := coalesce(p_send_gaps_mail, extract(hour from (now() at time zone 'Asia/Kolkata')) = 15);

  select ds.decrypted_secret into v_token from vault.decrypted_secrets ds where ds.name = 'github_bc_sync_dispatch';
  if v_token is null then
    insert into public.mp_sync_dispatch_log (send_gaps_mail, skipped_reason)
    values (v_mail, 'no github_bc_sync_dispatch secret in the vault');
    return jsonb_build_object('dispatched', false, 'reason', 'no token');
  end if;

  -- workflow_dispatch inputs travel as strings; the workflow compares github.event.inputs.send_gaps_mail == 'true'
  v_id := net.http_post(
    url := 'https://api.github.com/repos/keyur-tbd/marketplace-ads-pipeline/actions/workflows/marketplace-sync.yml/dispatches',
    body := jsonb_build_object('ref', 'main',
                               'inputs', jsonb_build_object('send_gaps_mail', case when v_mail then 'true' else 'false' end)),
    headers := jsonb_build_object('Authorization', 'Bearer ' || v_token,
                                  'Accept', 'application/vnd.github+json',
                                  'X-GitHub-Api-Version', '2022-11-28',
                                  'User-Agent', 'supabase-pg-cron-mp-sync',
                                  'Content-Type', 'application/json'),
    timeout_milliseconds := 15000);
  insert into public.mp_sync_dispatch_log (send_gaps_mail, net_request_id) values (v_mail, v_id);
  return jsonb_build_object('dispatched', true, 'net_request_id', v_id, 'send_gaps_mail', v_mail);
end
$function$;
revoke execute on function public.mp_sync_dispatch(boolean) from public, anon, authenticated;

select cron.unschedule(jobname) from cron.job where jobname = 'mp-sync-dispatch';
-- pg_cron's clock is GMT: 01:30 = 07:00 IST, 05:30 = 11:00, 09:30 = 15:00, 13:30 = 19:00, 17:30 = 23:00, 21:30 = 03:00
select cron.schedule('mp-sync-dispatch', '30 1,5,9,13,17,21 * * *',
  $$select public.mp_sync_dispatch()$$);
