import Config

# Set Logger to only output errors and above
config :logger, :console,
  level: :error,
  format: "\n$time $metadata[$level] $message\n",
  metadata: [:request_id]

#
# config :kvstore,
#   broadcast: fn message -> IO.inspect(message) end
