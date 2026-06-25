# credo:disable-for-this-file
defmodule Prism.PrismTarget do
  @moduledoc false
  defstruct channel_id: "",
            webhook_id: "",
            webhook_token: "",
            guild_id: nil,
            hub_id: nil,
            thread_id: nil,
            message_id: nil,
            overrides: nil,
            connection_id: nil,
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
        |> encode_guild_id(msg)
        |> encode_hub_id(msg)
        |> encode_thread_id(msg)
        |> encode_message_id(msg)
        |> encode_connection_id(msg)
        |> encode_channel_id(msg)
        |> encode_webhook_id(msg)
        |> encode_webhook_token(msg)
        |> encode_overrides(msg)
        |> encode_unknown_fields(msg)
      end
    )

    []

    [
      defp encode_channel_id(acc, msg) do
        try do
          if msg.channel_id == "" do
            acc
          else
            [acc, "\n", Protox.Encode.encode_string(msg.channel_id)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:channel_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_webhook_id(acc, msg) do
        try do
          if msg.webhook_id == "" do
            acc
          else
            [acc, "\x12", Protox.Encode.encode_string(msg.webhook_id)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:webhook_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_webhook_token(acc, msg) do
        try do
          if msg.webhook_token == "" do
            acc
          else
            [acc, "\x1A", Protox.Encode.encode_string(msg.webhook_token)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:webhook_token, "invalid field value"),
                    __STACKTRACE__
        end
      end,
      defp encode_guild_id(acc, msg) do
        try do
          case msg.guild_id do
            nil -> [acc]
            child_field_value -> [acc, "\"", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:guild_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_hub_id(acc, msg) do
        try do
          case msg.hub_id do
            nil -> [acc]
            child_field_value -> [acc, "*", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:hub_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_thread_id(acc, msg) do
        try do
          case msg.thread_id do
            nil -> [acc]
            child_field_value -> [acc, "2", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:thread_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_message_id(acc, msg) do
        try do
          case msg.message_id do
            nil -> [acc]
            child_field_value -> [acc, ":", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:message_id, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_overrides(acc, msg) do
        try do
          if msg.overrides == nil do
            acc
          else
            [acc, "B", Protox.Encode.encode_message(msg.overrides)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:overrides, "invalid field value"), __STACKTRACE__
        end
      end,
      defp encode_connection_id(acc, msg) do
        try do
          case msg.connection_id do
            nil -> [acc]
            child_field_value -> [acc, "J", Protox.Encode.encode_string(child_field_value)]
          end
        rescue
          ArgumentError ->
            reraise Protox.EncodingError.new(:connection_id, "invalid field value"),
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
          parse_key_value(bytes, struct(Prism.PrismTarget))
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
              {[channel_id: Protox.Decode.validate_string!(delimited)], rest}

            {2, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[webhook_id: Protox.Decode.validate_string!(delimited)], rest}

            {3, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[webhook_token: Protox.Decode.validate_string!(delimited)], rest}

            {4, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[guild_id: Protox.Decode.validate_string!(delimited)], rest}

            {5, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[hub_id: Protox.Decode.validate_string!(delimited)], rest}

            {6, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[thread_id: Protox.Decode.validate_string!(delimited)], rest}

            {7, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[message_id: Protox.Decode.validate_string!(delimited)], rest}

            {8, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)

              {[
                 overrides:
                   Protox.MergeMessage.merge(
                     msg.overrides,
                     Google.Protobuf.Struct.decode!(delimited)
                   )
               ], rest}

            {9, _, bytes} ->
              {len, bytes} = Protox.Varint.decode(bytes)
              {delimited, rest} = Protox.Decode.parse_delimited(bytes, len)
              {[connection_id: Protox.Decode.validate_string!(delimited)], rest}

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
        Prism.PrismTarget,
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
        1 => {:channel_id, {:scalar, ""}, :string},
        2 => {:webhook_id, {:scalar, ""}, :string},
        3 => {:webhook_token, {:scalar, ""}, :string},
        4 => {:guild_id, {:oneof, :_guild_id}, :string},
        5 => {:hub_id, {:oneof, :_hub_id}, :string},
        6 => {:thread_id, {:oneof, :_thread_id}, :string},
        7 => {:message_id, {:oneof, :_message_id}, :string},
        8 => {:overrides, {:scalar, nil}, {:message, Google.Protobuf.Struct}},
        9 => {:connection_id, {:oneof, :_connection_id}, :string}
      }
    end

    @deprecated "Use fields_defs()/0 instead"
    @spec defs_by_name() :: %{
            required(atom) => {non_neg_integer, Protox.Types.kind(), Protox.Types.type()}
          }
    def defs_by_name() do
      %{
        channel_id: {1, {:scalar, ""}, :string},
        connection_id: {9, {:oneof, :_connection_id}, :string},
        guild_id: {4, {:oneof, :_guild_id}, :string},
        hub_id: {5, {:oneof, :_hub_id}, :string},
        message_id: {7, {:oneof, :_message_id}, :string},
        overrides: {8, {:scalar, nil}, {:message, Google.Protobuf.Struct}},
        thread_id: {6, {:oneof, :_thread_id}, :string},
        webhook_id: {2, {:scalar, ""}, :string},
        webhook_token: {3, {:scalar, ""}, :string}
      }
    end
  )

  (
    @spec fields_defs() :: list(Protox.Field.t())
    def fields_defs() do
      [
        %{
          __struct__: Protox.Field,
          json_name: "channelId",
          kind: {:scalar, ""},
          label: :optional,
          name: :channel_id,
          tag: 1,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "webhookId",
          kind: {:scalar, ""},
          label: :optional,
          name: :webhook_id,
          tag: 2,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "webhookToken",
          kind: {:scalar, ""},
          label: :optional,
          name: :webhook_token,
          tag: 3,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "guildId",
          kind: {:oneof, :_guild_id},
          label: :proto3_optional,
          name: :guild_id,
          tag: 4,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "hubId",
          kind: {:oneof, :_hub_id},
          label: :proto3_optional,
          name: :hub_id,
          tag: 5,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "threadId",
          kind: {:oneof, :_thread_id},
          label: :proto3_optional,
          name: :thread_id,
          tag: 6,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "messageId",
          kind: {:oneof, :_message_id},
          label: :proto3_optional,
          name: :message_id,
          tag: 7,
          type: :string
        },
        %{
          __struct__: Protox.Field,
          json_name: "overrides",
          kind: {:scalar, nil},
          label: :optional,
          name: :overrides,
          tag: 8,
          type: {:message, Google.Protobuf.Struct}
        },
        %{
          __struct__: Protox.Field,
          json_name: "connectionId",
          kind: {:oneof, :_connection_id},
          label: :proto3_optional,
          name: :connection_id,
          tag: 9,
          type: :string
        }
      ]
    end

    [
      @spec(field_def(atom) :: {:ok, Protox.Field.t()} | {:error, :no_such_field}),
      (
        def field_def(:channel_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "channelId",
             kind: {:scalar, ""},
             label: :optional,
             name: :channel_id,
             tag: 1,
             type: :string
           }}
        end

        def field_def("channelId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "channelId",
             kind: {:scalar, ""},
             label: :optional,
             name: :channel_id,
             tag: 1,
             type: :string
           }}
        end

        def field_def("channel_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "channelId",
             kind: {:scalar, ""},
             label: :optional,
             name: :channel_id,
             tag: 1,
             type: :string
           }}
        end
      ),
      (
        def field_def(:webhook_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "webhookId",
             kind: {:scalar, ""},
             label: :optional,
             name: :webhook_id,
             tag: 2,
             type: :string
           }}
        end

        def field_def("webhookId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "webhookId",
             kind: {:scalar, ""},
             label: :optional,
             name: :webhook_id,
             tag: 2,
             type: :string
           }}
        end

        def field_def("webhook_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "webhookId",
             kind: {:scalar, ""},
             label: :optional,
             name: :webhook_id,
             tag: 2,
             type: :string
           }}
        end
      ),
      (
        def field_def(:webhook_token) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "webhookToken",
             kind: {:scalar, ""},
             label: :optional,
             name: :webhook_token,
             tag: 3,
             type: :string
           }}
        end

        def field_def("webhookToken") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "webhookToken",
             kind: {:scalar, ""},
             label: :optional,
             name: :webhook_token,
             tag: 3,
             type: :string
           }}
        end

        def field_def("webhook_token") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "webhookToken",
             kind: {:scalar, ""},
             label: :optional,
             name: :webhook_token,
             tag: 3,
             type: :string
           }}
        end
      ),
      (
        def field_def(:guild_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "guildId",
             kind: {:oneof, :_guild_id},
             label: :proto3_optional,
             name: :guild_id,
             tag: 4,
             type: :string
           }}
        end

        def field_def("guildId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "guildId",
             kind: {:oneof, :_guild_id},
             label: :proto3_optional,
             name: :guild_id,
             tag: 4,
             type: :string
           }}
        end

        def field_def("guild_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "guildId",
             kind: {:oneof, :_guild_id},
             label: :proto3_optional,
             name: :guild_id,
             tag: 4,
             type: :string
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
             tag: 5,
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
             tag: 5,
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
             tag: 5,
             type: :string
           }}
        end
      ),
      (
        def field_def(:thread_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "threadId",
             kind: {:oneof, :_thread_id},
             label: :proto3_optional,
             name: :thread_id,
             tag: 6,
             type: :string
           }}
        end

        def field_def("threadId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "threadId",
             kind: {:oneof, :_thread_id},
             label: :proto3_optional,
             name: :thread_id,
             tag: 6,
             type: :string
           }}
        end

        def field_def("thread_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "threadId",
             kind: {:oneof, :_thread_id},
             label: :proto3_optional,
             name: :thread_id,
             tag: 6,
             type: :string
           }}
        end
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
             tag: 7,
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
             tag: 7,
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
             tag: 7,
             type: :string
           }}
        end
      ),
      (
        def field_def(:overrides) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "overrides",
             kind: {:scalar, nil},
             label: :optional,
             name: :overrides,
             tag: 8,
             type: {:message, Google.Protobuf.Struct}
           }}
        end

        def field_def("overrides") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "overrides",
             kind: {:scalar, nil},
             label: :optional,
             name: :overrides,
             tag: 8,
             type: {:message, Google.Protobuf.Struct}
           }}
        end

        []
      ),
      (
        def field_def(:connection_id) do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "connectionId",
             kind: {:oneof, :_connection_id},
             label: :proto3_optional,
             name: :connection_id,
             tag: 9,
             type: :string
           }}
        end

        def field_def("connectionId") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "connectionId",
             kind: {:oneof, :_connection_id},
             label: :proto3_optional,
             name: :connection_id,
             tag: 9,
             type: :string
           }}
        end

        def field_def("connection_id") do
          {:ok,
           %{
             __struct__: Protox.Field,
             json_name: "connectionId",
             kind: {:oneof, :_connection_id},
             label: :proto3_optional,
             name: :connection_id,
             tag: 9,
             type: :string
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
    def default(:channel_id) do
      {:ok, ""}
    end,
    def default(:webhook_id) do
      {:ok, ""}
    end,
    def default(:webhook_token) do
      {:ok, ""}
    end,
    def default(:guild_id) do
      {:error, :no_default_value}
    end,
    def default(:hub_id) do
      {:error, :no_default_value}
    end,
    def default(:thread_id) do
      {:error, :no_default_value}
    end,
    def default(:message_id) do
      {:error, :no_default_value}
    end,
    def default(:overrides) do
      {:ok, nil}
    end,
    def default(:connection_id) do
      {:error, :no_default_value}
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
