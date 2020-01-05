# EventDB - Event store

Package eventdb (event sourcing) is a small library for event sourcing

Event sourcing persists the state of a business entity such an Order or a
Customer as a sequence of state-changing events. Whenever the state of a
business entity changes, a new event is appended to the list of events.
Since saving an event is a single operation, it is inherently atomic.
The application reconstructs an entity’s current state by replaying the events.

Applications persist events in an event store, which is a database of events.
The store has an API for adding and retrieving an entity’s events.
The event store also behaves like a message broker. It provides an API that
enables services to subscribe to events. When a service saves an event in
the event store, it is delivered to all interested subscribers.

Some entities, such as a Customer, can have a large number of events.
In order to optimise loading, an application can periodically save a snapshot
of an entity’s current state. To reconstruct the current state,
the application finds the most recent snapshot and the events that have
occurred since that snapshot. As a result, there are fewer events to replay.

**There is no Delete**

It is not possible to jump into the time machine and say that an event never
occurred (eg: delete a previous event).
As such, it is necessary to model a delete explicitly as a new transaction.
e.g. InvoiceCreated, InvoiceUpdated, and InvoiceDeleted
