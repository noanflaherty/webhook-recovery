# Design Rationale

## Intro

I chose to build within Theme 3: Systems & Reliability because I believe it best showcases the types of technical
challenges I enjoy solving and the types of products I enjoy building.

I'm a backend-leaning product engineer that enjoys creating platforms as products. Some of the most fun I had at Vellum
was building out our enterprise AI Development platform, which other software companies used as the backend that powers
the AI features within their own end-user-facing products.

There's something about creating a platform that others build on top of and come to rely on that gives me fulfillment –
it often comes with a tight feedback loop with the end user, the responsibility of ensuring it works well, and the need
to evolve it as requirements and the market changes.

## What I built

I created a webhook delivery system designed for graceful recovery. If there is an outage on either the producer or
consumer side, then, upon recovery, not only is the backlog of events replayed from producer to consumer, but also:

1. The backlog is burned down fairly between consumers such that one consumer with a large backlog does not impact the
   burndown of another consumer's backlog; and
2. Consumers can define policies around which events are worth replaying in the first place

The end goal: Improve the developer experience for consumers of webhooks, and the reliability of the systems they build
to process webhook events, following a provider outage.

### Why I build it

Nearly every company I've worked at has integrated with vendor webhooks and provided webhooks as an integration point of
our own. As both someone who had built webhooks and consumed them, I often found myself dissatisfied with the
surrounding tooling.

As a customer of webhooks, I was always frustrated when my vendor had an outage and, upon recovery, my services were
bombarded with delayed, replayed events, many of which I no longer cared about. This backlog of stale events clogged up
my queues and could take hours to churn through before the queues reach manageable sizes again and could begin
processing fresh events once more.

As a developer of webhooks, I was always surprised by how much code it took to build the same thing again and again in
house. More recently, off-the-shelf products like Svix and Hookdeck have come out and help with many of the basics, but
I still haven't seen any of these go deep into improving the lives of consumers following a provider outage.

### What's unique

Many home-grown webhook solutions and webhook infra products offer event retries and replays following provider/consumer
outages, but none that I've seen optimize for the webhook consumer's experience following an outage. Sure, the outage is
bad – you're missing live data – but the hours following the outage can sometimes be worse: clogged up queues, delays
before you can process fresh events again, overwhelmed workers, etc.

This is a webhook infra system that puts the consumer first.



