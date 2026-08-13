# Database webhooks

A row changes and something on the internet is told about it, without a single line of application code in between.

On Supabase that is a trigger calling `supabase_functions.http_request()`, which queues a request through pg_net, which makes the call from a background worker.
zou has the same two schemas and the same functions, and the call is made by the server rather than by a worker in the database.

## Writing one

The dashboard writes this for you, and this is what it writes.

```sql
create trigger orders_webhook after insert on public.orders
    for each row execute function supabase_functions.http_request(
        'https://example.com/hooks/orders',
        'POST',
        '{"Content-Type":"application/json"}',
        '{}',
        '1000'
    );
```

Five arguments, all of them text, in that order: the url, the method, the headers as a json object, the query parameters as a json object, and the timeout in milliseconds.
The last three can be left off, and `'null'` in any of them means the same as leaving it off.
A `POST` sends the row as a json body, and a `GET` sends nothing and carries only the parameters.

The body of a `POST` is the shape every webhook example reads.

```json
{
  "type": "INSERT",
  "table": "orders",
  "schema": "public",
  "record": { "id": 1, "total": 4200 },
  "old_record": null
}
```

`record` is null on a delete, `old_record` is null on an insert, and on an update `old_record` is whatever the table's replica identity carries, which for a table nobody has changed is the primary key and nothing else.
That is a trigger's `OLD`, so it is the whole row on any table, unlike the `old_record` a realtime subscriber gets.

Every request that is queued gets a row in `supabase_functions.hooks` naming the table, the trigger and the request, which is the audit trail upstream keeps and the way to find a request's id from sql.

## What happened to it

The answer lands in `net._http_response`, keyed by the id the request was queued under.

```sql
select r.status_code, r.content, r.error_msg
  from supabase_functions.hooks h
  join net._http_response r on r.id = h.request_id
 where h.hook_name = 'orders_webhook'
 order by h.id desc
 limit 10;
```

A request that got an answer has a status, a content type, the response headers as jsonb and the body as text.
A request that got nothing has `error_msg` instead, and `timed_out` says which kind of nothing it was.
Rows are kept six hours, which is pg_net's own `pg_net.ttl` default, and swept hourly.

Anything still in `net.http_request_queue` has not been delivered yet.
That is worth saying twice because upstream it means something else: pg_net's worker deletes the row when it picks it up, so a request in flight there is in neither table, and here it stays in the queue until it is finished with.

## Retries

Upstream makes one call and writes down what happened.
A receiver that was restarting when the row was written never hears about it, and nothing anywhere says so except an `error_msg` nobody is watching.

Here a request is tried three times, two seconds apart and then ten.
An attempt is repeated when nothing came back at all, when it timed out, and when the receiver answered 408, 429 or anything in the 500s, which is the receiver saying it could not take it right now.
Any other answer is the receiver having meant it: a 404 or a 401 is a real answer, and repeating it just puts four of the same line in somebody's log.

`net._http_response` gets one row per request, written when the tries are done, describing the last attempt.
So a request that failed twice and worked on the third try has a row with a 200 in it, and a request that never worked has the last failure.

**A receiver has to be idempotent.** A request that timed out may well have been handled, and the only thing this server can see is that no answer arrived, so the second attempt may be the second time that receiver has heard about the same row.
Two environment variables move it:

| Variable | Default | What it is |
| --- | --- | --- |
| `ZOU_WEBHOOK_ATTEMPTS` | `3` | How many times in total a request is tried. `1` is pg_net's own behaviour, which is to try once and record whatever happened. |
| `ZOU_WEBHOOK_BACKOFF_SECONDS` | `2` | The wait before the second attempt. Each one after that waits five times longer. |

## How it runs

There is no background worker, because a worker per database is a process running while nothing is happening, which is the thing this server is built not to have.

`net.http_post()` queues the row and calls `net.wake()`, which is a `pg_notify`, and the server is listening.
The notification is transactional, so a webhook fires when its transaction commits and never when it rolls back, which is also what makes a trigger safe next to a statement that might not commit.
Nothing is listening until the first request arrives at the front door, because a queue only fills because a transaction committed and a transaction only happened because somebody was there.
A row queued while the server was asleep is picked up when it wakes, so a scale to zero project delivers late rather than not at all.

Rows are claimed `for update skip locked` under a lease, so two nodes serving one project take different requests, and a node that died holding one has it taken by another a minute after that request's own timeout ran out.
Sixty four requests are in flight at once.

A response bigger than a megabyte is recorded as a failed request saying so, rather than truncated.
Upstream has no such limit, because curl streams into a buffer that worker owns; here a batch of answers is in memory at once.

## pg_net

The `net` schema is the pg_net interface, not the extension, and a project that calls it directly rather than through a trigger works the same way.

```sql
select net.http_post(
    url := 'https://example.com/hooks/manual',
    body := jsonb_build_object('hello', 'there'),
    headers := '{"Content-Type": "application/json"}'::jsonb,
    timeout_milliseconds := 2000
);
```

`net.http_get`, `net.http_post`, `net.http_delete`, `net.http_collect_response`, `net._http_collect_response`, `net._await_response`, `net._urlencode_string`, `net._encode_url_with_params_array`, `net.wake`, `net.check_worker_is_up`, `net.wait_until_running` and `net.worker_restart` are all there with upstream's signatures and defaults, including the refusal of a `Content-Type` on a `POST` that is not `application/json` and the three url errors curl reports through pg_net.
The three about the worker do nothing here and say nothing is wrong, because there is no worker to be down.

A database that really has the pg_net extension installed keeps it: the schema is left exactly as the extension made it, and this server's dispatcher stays out of a queue that already has a worker draining it.
Two differences that are visible from sql:

* `net.http_collect_response` returns the result. Upstream's is deprecated and its body selects into nothing, so calling it there raises rather than answering.
* The scheme of a url is passed through as written. Upstream's encoder is curl's parser and normalises `HTTP://` to `http://` on the way into the queue.

## Scheduled jobs

A webhook that fires on a clock rather than on a row is a job, and jobs are the `cron` schema, described in [cron.md](cron.md).
It is the same dispatcher with a different reason for the row existing, so a job whose command calls `net.http_post()` is a scheduled webhook and needs nothing else.
