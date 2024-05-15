# Overview
`execlib` is a lightweight multi-threaded job framework written in Python. It implements a
simple event-based model over core Python utilities like `ThreadPoolExecutor` to
facilitate reactivity and manage concurrent responses.

There are a few top-level classes exposed by the package:

- **Router**: Central event routing object. Routers facilitate route registration,
  allowing for _pattern_-based matching of _events_ to arbitrary _callback_ functions. For
  example, you could have a function that converts a PDF file to a collection images
  (_callback_), and want this function to be called for a new files (_event_) that match
  the glob `*.pdf` (_pattern_).
- **Listener**: Connective event listening object, often created directly by router
  instances. Listeners pay attention to events arising along registered routes of an
  affiliated router, passing them through (after optional delays, debouncing, filtering,
  etc). In the above example, the associated `Listener` instance might wrap a tool like
  iNotify to dynamically respond to file events.
- **Server**: Long-running process manager for listeners and optional live-reloading via
  HTTP. Interfaces with listener `start()` and `shutdown()` for graceful interruption.
