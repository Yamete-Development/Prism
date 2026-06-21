defmodule Prism.RateLimit.HeadersTest do
  use ExUnit.Case, async: true

  alias Prism.RateLimit.Headers

  describe "extract_int/2" do
    test "extracts integer from headers by case-insensitive name" do
      headers = [{"X-RateLimit-Limit", "10"}]
      assert Headers.extract_int(headers, "x-ratelimit-limit") == 10
    end

    test "handles mixed-case header names" do
      headers = [{"X-RateLimit-ReMaInInG", "5"}]
      assert Headers.extract_int(headers, "x-ratelimit-remaining") == 5
    end

    test "returns nil when header is missing" do
      headers = [{"other", "value"}]
      assert Headers.extract_int(headers, "x-ratelimit-limit") == nil
    end

    test "returns nil when empty headers list" do
      assert Headers.extract_int([], "x-ratelimit-limit") == nil
    end

    test "returns nil when value is not an integer" do
      headers = [{"x-ratelimit-limit", "not_an_int"}]
      assert Headers.extract_int(headers, "x-ratelimit-limit") == nil
    end

    test "parses integer prefix of float-like string" do
      # Integer.parse("1.5") returns {1, ".5"} in Elixir
      headers = [{"x-ratelimit-limit", "1.5"}]
      assert Headers.extract_int(headers, "x-ratelimit-limit") == 1
    end

    test "extracts first matching header when duplicates exist" do
      headers = [{"x-ratelimit-limit", "10"}, {"x-ratelimit-limit", "20"}]
      assert Headers.extract_int(headers, "x-ratelimit-limit") == 10
    end
  end

  describe "extract_float/2" do
    test "extracts float from headers by case-insensitive name" do
      headers = [{"X-RateLimit-Reset-After", "1.234"}]
      assert Headers.extract_float(headers, "x-ratelimit-reset-after") == 1.234
    end

    test "handles integer-value strings as floats" do
      headers = [{"x-ratelimit-reset-after", "2"}]
      assert Headers.extract_float(headers, "x-ratelimit-reset-after") == 2.0
    end

    test "returns nil when header is missing" do
      headers = [{"other", "value"}]
      assert Headers.extract_float(headers, "x-ratelimit-reset-after") == nil
    end

    test "returns nil when empty headers list" do
      assert Headers.extract_float([], "x-ratelimit-reset-after") == nil
    end

    test "returns nil when value is not a valid float" do
      headers = [{"x-ratelimit-reset-after", "not_a_float"}]
      assert Headers.extract_float(headers, "x-ratelimit-reset-after") == nil
    end

    test "extracts first matching header when duplicates exist" do
      headers = [{"x-ratelimit-reset-after", "1.5"}, {"x-ratelimit-reset-after", "2.5"}]
      assert Headers.extract_float(headers, "x-ratelimit-reset-after") == 1.5
    end
  end

  describe "parse_2xx/1" do
    test "returns {limit, remaining, reset_at_ms} when all headers present" do
      headers = [
        {"x-ratelimit-limit", "10"},
        {"x-ratelimit-remaining", "5"},
        {"x-ratelimit-reset-after", "1.5"}
      ]
      {limit, remaining, reset_at_ms} = Headers.parse_2xx(headers)
      assert limit == 10
      assert remaining == 5
      assert is_integer(reset_at_ms)
    end

    test "returns nil when limit header is missing" do
      headers = [{"x-ratelimit-remaining", "5"}, {"x-ratelimit-reset-after", "1.5"}]
      assert Headers.parse_2xx(headers) == nil
    end

    test "returns nil when remaining header is missing" do
      headers = [{"x-ratelimit-limit", "10"}, {"x-ratelimit-reset-after", "1.5"}]
      assert Headers.parse_2xx(headers) == nil
    end

    test "returns nil when reset-after header is missing" do
      headers = [{"x-ratelimit-limit", "10"}, {"x-ratelimit-remaining", "5"}]
      assert Headers.parse_2xx(headers) == nil
    end

    test "returns nil when all headers are missing" do
      assert Headers.parse_2xx([]) == nil
    end

    test "handles remaining value of 0" do
      headers = [
        {"x-ratelimit-limit", "10"},
        {"x-ratelimit-remaining", "0"},
        {"x-ratelimit-reset-after", "1.0"}
      ]
      {limit, remaining, _} = Headers.parse_2xx(headers)
      assert limit == 10
      assert remaining == 0
    end

    test "reset_at_ms is roughly now + 2000 when reset_after_sec is 2.0" do
      headers = [
        {"x-ratelimit-limit", "10"},
        {"x-ratelimit-remaining", "9"},
        {"x-ratelimit-reset-after", "2.0"}
      ]
      now = Headers.now_ms()
      {_limit, _remaining, reset_at_ms} = Headers.parse_2xx(headers)
      assert_in_delta reset_at_ms, now + 2000, 100
    end
  end

  describe "parse_429/2" do
    test "parses Discord 429 JSON with retry_after field" do
      body = Jason.encode!(%{"retry_after" => 1.5, "global" => false, "message" => "rate limited"})
      headers = [{"x-ratelimit-limit", "10"}]
      result = Headers.parse_429(headers, body)
      assert result.retry_after_ms == 1500
      assert result.is_cloudflare == false
      assert result.is_global == false
      assert result.limit == 10
    end

    test "detects global rate limit from JSON global field" do
      body = Jason.encode!(%{"retry_after" => 2.0, "global" => true})
      headers = [{"x-ratelimit-limit", "10"}]
      result = Headers.parse_429(headers, body)
      assert result.is_global == true
    end

    test "detects global rate limit from message text" do
      body = Jason.encode!(%{"retry_after" => 1.0, "message" => "You are being rate limited due to global rate limits"})
      headers = [{"x-ratelimit-limit", "10"}]
      result = Headers.parse_429(headers, body)
      assert result.is_global == true
    end

    test "detects Cloudflare block from JSON code=0" do
      body = Jason.encode!(%{"retry_after" => 5.0, "code" => 0})
      headers = [{"x-ratelimit-limit", "10"}]
      result = Headers.parse_429(headers, body)
      assert result.is_cloudflare == true
      assert result.retry_after_ms == 5000
    end

    test "detects Cloudflare block from non-JSON (HTML) body" do
      body = "<html><body>Cloudflare rate limiting</body></html>"
      headers = [{"retry-after", "30"}]
      result = Headers.parse_429(headers, body)
      assert result.is_cloudflare == true
      assert result.retry_after_ms == 30000
    end

    test "falls back to retry-after header when JSON has no retry_after" do
      body = Jason.encode!(%{"message" => "rate limited"})
      headers = [{"retry-after", "3.0"}]
      result = Headers.parse_429(headers, body)
      assert result.retry_after_ms == 3000
    end

    test "defaults to 5000ms when no retry information is available" do
      body = Jason.encode!(%{"message" => "rate limited"})
      headers = []
      result = Headers.parse_429(headers, body)
      assert result.retry_after_ms == 5000
    end

    test "extracts bucket and scope from headers" do
      body = Jason.encode!(%{"retry_after" => 1.0})
      headers = [{"x-ratelimit-bucket", "abc123"}, {"x-ratelimit-scope", "user"}]
      result = Headers.parse_429(headers, body)
      assert result.bucket == "abc123"
      assert result.scope == "user"
    end

    test "detects global from x-ratelimit-global header" do
      body = Jason.encode!(%{"retry_after" => 1.0, "global" => false})
      headers = [{"x-ratelimit-global", "true"}]
      result = Headers.parse_429(headers, body)
      assert result.is_global == true
    end

    test "detects global from scope header set to global" do
      body = Jason.encode!(%{"retry_after" => 1.0, "global" => false})
      headers = [{"x-ratelimit-scope", "global"}]
      result = Headers.parse_429(headers, body)
      assert result.is_global == true
    end

    test "non-JSON body with retry-after header is treated as Cloudflare" do
      body = "cloudflare error page"
      headers = [{"retry-after", "60"}]
      result = Headers.parse_429(headers, body)
      assert result.is_cloudflare == true
      assert result.retry_after_ms == 60000
    end

    test "non-JSON body without retry-after defaults to 5000ms Cloudflare" do
      body = "some error"
      headers = []
      result = Headers.parse_429(headers, body)
      assert result.is_cloudflare == true
      assert result.retry_after_ms == 5000
    end

    test "JSON body with integer retry_after" do
      body = Jason.encode!(%{"retry_after" => 2})
      headers = [{"x-ratelimit-limit", "10"}]
      result = Headers.parse_429(headers, body)
      assert result.retry_after_ms == 2000
    end
  end

  describe "cloudflare_response?/2" do
    test "detects cf-ray header" do
      assert Headers.cloudflare_response?([{"cf-ray", "abc123def"}], "") == true
    end

    test "detects cloudflare server header" do
      assert Headers.cloudflare_response?([{"server", "cloudflare"}], "") == true
    end

    test "detects cloudflare in server header (mixed case)" do
      assert Headers.cloudflare_response?([{"Server", "CloudFlare-Nginx"}], "") == true
    end

    test "detects cloudflare in body text" do
      assert Headers.cloudflare_response?([], "Error from Cloudflare") == true
    end

    test "returns false for normal Discord response" do
      headers = [{"x-ratelimit-limit", "10"}, {"content-type", "application/json"}]
      body = Jason.encode!(%{"retry_after" => 1.0})
      assert Headers.cloudflare_response?(headers, body) == false
    end

    test "returns false for empty headers and body" do
      assert Headers.cloudflare_response?([], "") == false
    end

    test "handles map format headers" do
      assert Headers.cloudflare_response?(%{"cf-ray" => "abc123"}, "") == true
    end

    test "handles map format headers with no cloudflare" do
      assert Headers.cloudflare_response?(%{"x-ratelimit-limit" => "10"}, "") == false
    end

    test "detects cloudflare with :server atom header key converted to string" do
      assert Headers.cloudflare_response?([{:server, "cloudflare"}], "")
    end
  end
end
