payload_json = """
{"trace_headers":{"traceparent":"00-00000000000000000000000000000000-0000000000000000-00"},"a":"execute","b":"uuid123","t":[{"c":"123"}],"p":{}}
"""
{:ok, payload} = Jason.decode(payload_json)
IO.inspect(Prism.FanoutBroadway.KeyExpansion.expand_keys(payload))
