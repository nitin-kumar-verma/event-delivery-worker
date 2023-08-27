This is a worker micro-service for event delivery. This service can be scaled horizontally according to loads.

The worker polls 'queued_events' list from redis and pushes it to 'processing_events' list, in a single transaction,
thus ensuring one worker owns processing for one event.

If a worker instance fails durings the processing, the event will remain in 'processing_events' list.

There will be another worker(event-delivery-job) which runs on scheduled intervals and it polls 'processing_events' list
for events which were pushed before a certain timestamp, and pushed them back to 'queued_events' so that they can be reprocessed.

The endpoints "https://event-delivery-dest-1.deno.dev/", "https://event-delivery-dest-2.deno.dev/", "https://event-delivery-dest-3.deno.dev/" 
is deployed on deno and it returns success/failure randomly with statuscode 200 and 500 with random delays upto 100ms to emulate the use-cases requested.