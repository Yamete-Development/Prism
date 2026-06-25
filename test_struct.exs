defmodule TestStruct do
  def run do
    require Logger
    struct_pb = %Google.Protobuf.Struct{
      fields: %{
        "type" => %Google.Protobuf.Value{kind: {:number_value, 1.0}},
        "style" => %Google.Protobuf.Value{kind: {:number_value, 2.0}},
        "label" => %Google.Protobuf.Value{kind: {:string_value, "hello"}},
        "components" => %Google.Protobuf.Value{
          kind: {:list_value, %Google.Protobuf.ListValue{
            values: [
              %Google.Protobuf.Value{
                kind: {:struct_value, %Google.Protobuf.Struct{
                  fields: %{
                    "type" => %Google.Protobuf.Value{kind: {:number_value, 2.0}}
                  }
                }}
              }
            ]
          }}
        }
      }
    }
    
    IO.inspect(Prism.Helpers.struct_to_map(struct_pb))
  end
end

TestStruct.run()
