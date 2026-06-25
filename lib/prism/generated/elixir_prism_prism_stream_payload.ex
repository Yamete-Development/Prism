# credo:disable-for-this-file
defmodule Prism.PrismStreamPayload do
  @moduledoc false
  defstruct batch_id: "",
            action: "",
            payload: "",
            targets: [],
            message_id: nil,
            shard_index: nil,
            hub_id: nil,
            hub_name: nil,
            metadata: nil,
            payload_struct: nil,
            __uf__: []

  (
    (
      @spec encode(struct) :: {:ok, iodata} | {:error, any}
      def encode(msg) do
        try do
          {:ok, encode!(msg)}
        rescue
          e in [Protox.EncodingError, Protox.RequiredFieldsError] -> {:error, e}
        end
      end

      @spec encode!(struct) :: iodata | no_return
      def encode!(msg) do
        []
        |> encode_message_id(msg)
        |> encode_shard_index(msg)
        |> encode_hub_id(msg)
        |> encode_hub_name(msg)
        |> encode_batch_id(msg)
        |> encode_action(msg)
        |> encode_payload(msg)
        |> encode_targets(msg)
        |> encode_metadata(msg)
        |> encode_payload_struct(msg)
        |> encode_unknown_fields(msg)
      end
    )

    []

    [
      defp encode_batch_id(acc, msg) do
        try do
          if msg.batch_id == "" do
            acc
          else
            [acc, "\n", Protox.Encode.encode_string(msg.batch_id)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:batch_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_action(acc, msg) do
        try do
          if msg.action == "" do
            acc
          else
            [acc, "\x12", Protox.Encode.encode_string(msg.action)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:action, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_payload(acc, msg) do
        try do
          if msg.payload == "" do
            acc
          else
            [acc, "\x1A", Protox.Encode.encode_string(msg.payload)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:payload, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_targets(acc, msg) do
        try do
          case msg.targets do
            [] ->
              acc

            values ->
              [
                acc,
                Enum.reduce(values, [], fn value, acc ->
                  [acc, "\"", Protox.Encode.encode_message(value)]
                end)
              ]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:targets, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_message_id(acc, msg) do
        try do
          case msg.message_id do
            nil -> [acc]
            child_field_value -> [acc, "*", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:message_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_shard_index(acc, msg) do
        try do
          case msg.shard_index do
            nil -> [acc]
            child_field_value -> [acc, "0", Protox.Encode.encode_int32(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:shard_index, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_hub_id(acc, msg) do
        try do
          case msg.hub_id do
            nil -> [acc]
            child_field_value -> [acc, ":", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:hub_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_hub_name(acc, msg) do
        try do
          case msg.hub_name do
            nil -> [acc]
            child_field_value -> [acc, "B", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:hub_name, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_metadata(acc, msg) do
        try do
          if msg.metadata == nil do
            acc
          else
            [acc, "J", Protox.Encode.encode_message(msg.metadata)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:metadata, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_payload_struct(acc, msg) do
        try do
          if msg.payload_struct == nil do
            acc
          else
            [acc, "R", Protox.Encode.encode_message(msg.payload_struct)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:payload_struct, "invalid field value"),
                    __STACKTRACE__
        end
      end
    ]

    defp encode_unknown_fields(acc, msg) do
      Enum.reduce(msg.__struct__.unknown_fields(msg), acc, fn {tag, wire_type, bytes}, acc ->
        case wire_type do
          0 ->
            [acc, Protox.Encode.make_key_bytes(tag, :int32), bytes]

          1 ->
            [acc, Protox.Encode.make_key_bytes(tag, :double), bytes]

          2 ->
            len_bytes = bytes |> byte_size() |> Protox.Varint.encode()
            [acc, Protox.Encode.make_key_bytes(tag, :packed), len_bytes, bytes]

          5 ->
            [acc, Protox.Encode.make_key_bytes(tag, :float), bytes]
        end
      end)
    end
  )

  (
    (
      @spec decode(binary) :: {:ok, struct} | {:error, any}
      def decode(bytes) do
        try do
          {:ok, decode!(bytes)}
        rescue
          e in [Protox.DecodingError, Protox.IllegalTagError, Protox.RequiredFieldsError] ->
            {:error, e}
        end
      end

      (
        @spec decode!(binary) :: struct | no_return
        def decode!(bytes) do
          parse_key_value(bytes, struct(Prism.PrismStreamPayload))
        end
      )
    )

    (
      @spec parse_key_value(binary, struct) :: struct
      defp parse_key_value(<<>>, msg) do
        msg
      end

      defp parse_key_value(bytes, msg) do
        {field, rest} =
          case Protox.Decode.parse_key(bytes) do
            {0, _, _} ->
              raise %Protox.IllegalTagError{}

            {1, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[batch_id: Protox.Decode.validate_string!(delimited)], rest}

            {2, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[action: Protox.Decode.validate_string!(delimited)], rest}

            {3, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[payload: Protox.Decode.validate_string!(delimited)], rest}

            {4, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[targets: msg.targets ++ [Prism.PrismTarget.decode!(delimited)]], rest}

            {5, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[message_id: Protox.Decode.validate_string!(delimited)], rest}

            {6, _, bytes} ->
              {value, rest} = Protox.Decode.parse_int32(bytes)
              {[shard_index: value], rest}

            {7, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[hub_id: Protox.Decode.validate_string!(delimited)], rest}

            {8, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[hub_name: Protox.Decode.validate_string!(delimited)], rest}

            {9, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)

              {[
                 metadata:
                   Protox.MergeMessage.merge(
                     msg.metadata,
                     Prism.PrismStreamMetadata.decode!(delimited)
                   )
               ], rest}

            {10, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)

              {[
                 payload_struct:
                   Protox.MergeMessage.merge(
                     msg.payload_struct,
                     Google.Protobuf.Struct.decode!(delimited)
                   )
               ], rest}

            {tag, wire_type, rest} ->
              {value, rest} = Protox.Decode.parse_unknown(tag, wire_type, rest)

              {[
                 {msg.__struct__.unknown_fields_name(),
                  [value | msg.__struct__.unknown_fields(msg)]}
               ], rest}
          end

        msg_updated = struct(msg, field)
        parse_key_value(rest, msg_updated)
      end
    )

    []
  )

  (
    @spec json_decode(iodata(), keyword()) :: {:ok, struct()} | {:error, any()}
    def json_decode(input, opts \\ []) do
      try do
        {:ok, json_decode!(input, opts)}
      rescue
        e in Protox.JsonDecodingError -> {:error, e}
      end
    end

    @spec json_decode!(iodata(), keyword()) :: struct() | no_return()
    def json_decode!(input, opts \\ []) do
      {json_library_wrapper, json_library} = Protox.JsonLibrary.get_library(opts, :decode)

      Protox.JsonDecode.decode!(
        input,
        Prism.PrismStreamPayload,
        &json_library_wrapper.decode!(json_library, &1)
      )
    end

    @spec json_encode(struct(), keyword()) :: {:ok, iodata()} | {:error, any()}
    def json_encode(msg, opts \\ []) do
      try do
        {:ok, json_encode!(msg, opts)}
      rescue
        e in Protox.JsonEncodingError -> {:error, e}
      end
    end

    @spec json_encode!(struct(), keyword()) :: iodata() | no_return()
    def json_encode!(msg, opts \\ []) do
      {json_library_wrapper, json_library} = Protox.JsonLibrary.get_library(opts, :encode)
      Protox.JsonEncode.encode!(msg, &json_library_wrapper.encode!(json_library, &1))
    end
  )

  (
    @deprecated "Use fields_defs()/0 instead"
    @spec defs() :: %{
            required(non_neg_integer) => {atom, Protox.Types.kind(), Protox.Types.type()}
          }
    def defs() do
      %{
        1 => {:batch_id, {:scalar, ""}, :string},
        2 => {:action, {:scalar, ""}, :string},
        3 => {:payload, {:scalar, ""}, :string},
        4 => {:targets, :unpacked, {:message, Prism.PrismTarget}},
        5 => {:message_id, {:oneof, :_message_id}, :string},
        6 => {:shard_index, {:oneof, :_shard_index}, :int32},
        7 => {:hub_id, {:oneof, :_hub_id}, :string},
        8 => {:hub_name, {:oneof, :_hub_name}, :string},
        9 => {:metadata, {:scalar, nil}, {:message, Prism.PrismStreamMetadata}},
        10 => {:payload_struct, {:scalar, nil}, {:message, Google.Protobuf.Struct}}
      }
    end

    @deprecated "Use fields_defs()/0 instead"
    @spec defs_by_name() :: %{
            required(atom) => {non_neg_integer, Protox.Types.kind(), Protox.Types.type()}
          }
    def defs_by_name() do
      %{
        action: {2, {:scalar, ""}, :string},
        batch_id: {1, {:scalar, ""}, :string},
        hub_id: {7, {:oneof, :_hub_id}, :string},
        hub_name: {8, {:oneof, :_hub_name}, :string},
        message_id: {5, {:oneof, :_message_id}, :string},
        metadata: {9, {:scalar, nil}, {:message, Prism.PrismStreamMetadata}},
        payload: {3, {:scalar, ""}, :string},
        payload_struct: {10, {:scalar, nil}, {:message, Google.Protobuf.Struct}},
        shard_index: {6, {:oneof, :_shard_index}, :int32},
        targets: {4, :unpacked, {:message, Prism.PrismTarget}}
      }
    end
  )

  (
    @spec fields_defs() :: list(Protox.Field.t())
    def fields_defs() do
      [
        %{
          __struct__: Protox.Field,
          json_name: "batchId",
          kind: {:scalar, ""},
          label: :optional,
          name: :batch_id,
          tag: 1,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "action",
          kind: {:scalar, ""},
          label: :optional,
          name: :action,
          tag: 2,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "payload",
          kind: {:scalar, ""},
          label: :optional,
          name: :payload,
          tag: 3,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "targets",
          kind: :unpacked,
          label: :repeated,
          name: :targets,
          tag: 4,
          type: {:message, Prism.PrismTarget}
        },
        %{
          __struct__: Protox.Field,
          json_name: "messageId",
          kind: {:oneof, :_message_id},
          label: :proto3_optional,
          name: :message_id,
          tag: 5,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "shardIndex",
          kind: {:oneof, :_shard_index},
          label: :proto3_optional,
          name: :shard_index,
          tag: 6,
          type: :int32
        },
        %{
          __struct__: Protox.Field,
          json_name: "hubId",
          kind: {:oneof, :_hub_id},
          label: :proto3_optional,
          name: :hub_id,
          tag: 7,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "hubName",
          kind: {:oneof, :_hub_name},
          label: :proto3_optional,
          name: :hub_name,
          tag: 8,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "metadata",
          kind: {:scalar, nil},
          label: :optional,
          name: :metadata,
          tag: 9,
          type: {:message, Prism.PrismStreamMetadata}
        },
        %{
          __struct__: Protox.Field,
          json_name: "payloadStruct",
          kind: {:scalar, nil},
          label: :optional,
          name: :payload_struct,
          tag: 10,
          type: {:message, Google.Protobuf.Struct}
        }
      ]
    end

    [
      @spec(field_def(atom) :: {:ok, Protox.Field.t()} | {:error, :no_such_field}),
      (
        def field_def(:batch_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "batchId",
             kind: {:scalar, ""},
             label: :optional,
             name: :batch_id,
             tag: 1,
             type: :string
           }}
        end

        def field_def("batchId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "batchId",
             kind: {:scalar, ""},
             label: :optional,
             name: :batch_id,
             tag: 1,
             type: :string
           }}
        end

        def field_def("batch_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "batchId",
             kind: {:scalar, ""},
             label: :optional,
             name: :batch_id,
             tag: 1,
             type: :string
           }}
        end
      ),
      (
        def field_def(:action) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "action",
             kind: {:scalar, ""},
             label: :optional,
             name: :action,
             tag: 2,
             type: :string
           }}
        end

        def field_def("action") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "action",
             kind: {:scalar, ""},
             label: :optional,
             name: :action,
             tag: 2,
             type: :string
           }}
        end

        []
      ),
      (
        def field_def(:payload) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "payload",
             kind: {:scalar, ""},
             label: :optional,
             name: :payload,
             tag: 3,
             type: :string
           }}
        end

        def field_def("payload") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "payload",
             kind: {:scalar, ""},
             label: :optional,
             name: :payload,
             tag: 3,
             type: :string
           }}
        end

        []
      ),
      (
        def field_def(:targets) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "targets",
             kind: :unpacked,
             label: :repeated,
             name: :targets,
             tag: 4,
             type: {:message, Prism.PrismTarget}
           }}
        end

        def field_def("targets") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "targets",
             kind: :unpacked,
             label: :repeated,
             name: :targets,
             tag: 4,
             type: {:message, Prism.PrismTarget}
           }}
        end

        []
      ),
      (
        def field_def(:message_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "messageId",
             kind: {:oneof, :_message_id},
             label: :proto3_optional,
             name: :message_id,
             tag: 5,
             type: :string
           }}
        end

        def field_def("messageId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "messageId",
             kind: {:oneof, :_message_id},
             label: :proto3_optional,
             name: :message_id,
             tag: 5,
             type: :string
           }}
        end

        def field_def("message_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "messageId",
             kind: {:oneof, :_message_id},
             label: :proto3_optional,
             name: :message_id,
             tag: 5,
             type: :string
           }}
        end
      ),
      (
        def field_def(:shard_index) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "shardIndex",
             kind: {:oneof, :_shard_index},
             label: :proto3_optional,
             name: :shard_index,
             tag: 6,
             type: :int32
           }}
        end

        def field_def("shardIndex") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "shardIndex",
             kind: {:oneof, :_shard_index},
             label: :proto3_optional,
             name: :shard_index,
             tag: 6,
             type: :int32
           }}
        end

        def field_def("shard_index") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "shardIndex",
             kind: {:oneof, :_shard_index},
             label: :proto3_optional,
             name: :shard_index,
             tag: 6,
             type: :int32
           }}
        end
      ),
      (
        def field_def(:hub_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "hubId",
             kind: {:oneof, :_hub_id},
             label: :proto3_optional,
             name: :hub_id,
             tag: 7,
             type: :string
           }}
        end

        def field_def("hubId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "hubId",
             kind: {:oneof, :_hub_id},
             label: :proto3_optional,
             name: :hub_id,
             tag: 7,
             type: :string
           }}
        end

        def field_def("hub_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "hubId",
             kind: {:oneof, :_hub_id},
             label: :proto3_optional,
             name: :hub_id,
             tag: 7,
             type: :string
           }}
        end
      ),
      (
        def field_def(:hub_name) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "hubName",
             kind: {:oneof, :_hub_name},
             label: :proto3_optional,
             name: :hub_name,
             tag: 8,
             type: :string
           }}
        end

        def field_def("hubName") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "hubName",
             kind: {:oneof, :_hub_name},
             label: :proto3_optional,
             name: :hub_name,
             tag: 8,
             type: :string
           }}
        end

        def field_def("hub_name") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "hubName",
             kind: {:oneof, :_hub_name},
             label: :proto3_optional,
             name: :hub_name,
             tag: 8,
             type: :string
           }}
        end
      ),
      (
        def field_def(:metadata) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "metadata",
             kind: {:scalar, nil},
             label: :optional,
             name: :metadata,
             tag: 9,
             type: {:message, Prism.PrismStreamMetadata}
           }}
        end

        def field_def("metadata") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "metadata",
             kind: {:scalar, nil},
             label: :optional,
             name: :metadata,
             tag: 9,
             type: {:message, Prism.PrismStreamMetadata}
           }}
        end

        []
      ),
      (
        def field_def(:payload_struct) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "payloadStruct",
             kind: {:scalar, nil},
             label: :optional,
             name: :payload_struct,
             tag: 10,
             type: {:message, Google.Protobuf.Struct}
           }}
        end

        def field_def("payloadStruct") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "payloadStruct",
             kind: {:scalar, nil},
             label: :optional,
             name: :payload_struct,
             tag: 10,
             type: {:message, Google.Protobuf.Struct}
           }}
        end

        def field_def("payload_struct") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "payloadStruct",
             kind: {:scalar, nil},
             label: :optional,
             name: :payload_struct,
             tag: 10,
             type: {:message, Google.Protobuf.Struct}
           }}
        end
      ),
      def field_def(_) do
        {:error, :no_such_field}
      end
    ]
  )

  (
    @spec unknown_fields(struct) :: [{non_neg_integer, Protox.Types.tag(), binary}]
    def unknown_fields(msg) do
      msg.__uf__
    end

    @spec unknown_fields_name() :: :__uf__
    def unknown_fields_name() do
      :__uf__
    end

    @spec clear_unknown_fields(struct) :: struct
    def clear_unknown_fields(msg) do
      struct!(msg, [{unknown_fields_name(), []}])
    end
  )

  (
    @spec required_fields() :: []
    def required_fields() do
      []
    end
  )

  (
    @spec syntax() :: atom()
    def syntax() do
      :proto3
    end
  )

  [
    @spec(default(atom) :: {:ok, boolean | integer | String.t() | float} | {:error, atom}),
    def default(:batch_id) do
      {:ok, ""}
    end,
    def default(:action) do
      {:ok, ""}
    end,
    def default(:payload) do
      {:ok, ""}
    end,
    def default(:targets) do
      {:error, :no_default_value}
    end,
    def default(:message_id) do
      {:error, :no_default_value}
    end,
    def default(:shard_index) do
      {:error, :no_default_value}
    end,
    def default(:hub_id) do
      {:error, :no_default_value}
    end,
    def default(:hub_name) do
      {:error, :no_default_value}
    end,
    def default(:metadata) do
      {:ok, nil}
    end,
    def default(:payload_struct) do
      {:ok, nil}
    end,
    def default(_) do
      {:error, :no_such_field}
    end
  ]

  (
    @spec file_options() :: nil
    def file_options() do
      nil
    end
  )
end
