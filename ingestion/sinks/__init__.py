"""Where a table's rows actually land. Never imports ``dlt`` or a
connector -- only pyarrow and the (duck-typed) backend/engine object it's
handed."""
