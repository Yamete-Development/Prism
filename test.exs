defmodule Test do
  def run do
    string = "{\"hello\":\"world\"}"
    binary = <<26, byte_size(string)>> <> string
    
    decoded = Prism.PrismStreamPayload.decode!(binary)
    IO.inspect(decoded, label: "decoded")
  end
end
Test.run()
