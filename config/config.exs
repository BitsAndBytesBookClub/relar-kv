import Config

# Set Logger to only output errors and above
config :logger, :console,
  level: :info,
  format: "\n$time $metadata[$level] $message\n",
  metadata: [:request_id]
