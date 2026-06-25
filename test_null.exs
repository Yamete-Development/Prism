defmodule TestNull do
  def run do
    struct_pb = %Google.Protobuf.Struct{
      fields: %{
        "emoji" => %Google.Protobuf.Value{
          kind: {:struct_value, %Google.Protobuf.Struct{
            fields: %{
              "id" => %Google.Protobuf.Value{kind: {:null_value, :NULL_VALUE}}
            }
          }}
        }
      }
    }
    
    map = Prism.Helpers.struct_to_map(struct_pb)
    IO.inspect(map)
    IO.inspect(Jason.encode!(map))
  end
end

TestNull.run()
