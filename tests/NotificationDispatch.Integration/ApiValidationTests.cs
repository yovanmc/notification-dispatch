using System.Net;
using System.Net.Http.Json;
using NotificationDispatch.Integration.Fixtures;

namespace NotificationDispatch.Integration;

public class ApiValidationTests : IClassFixture<AppFixture>
{
    private readonly AppFixture _app;

    public ApiValidationTests(AppFixture app)
    {
        _app = app;
    }

    private async Task<HttpResponseMessage> PostAsync(object body, string? idempotencyKey = null)
    {
        idempotencyKey ??= Guid.NewGuid().ToString();
        var request = new HttpRequestMessage(HttpMethod.Post, "/notifications")
        {
            Content = JsonContent.Create(body)
        };
        request.Headers.Add("Idempotency-Key", idempotencyKey);
        return await _app.ApiClient.SendAsync(request);
    }

    [Theory]
    [InlineData("fax")]
    [InlineData("telegram")]
    public async Task InvalidChannel_Returns400(string channel)
    {
        await _app.FlushRedisAsync();
        var response = await PostAsync(new { channel, recipient = "x@x.com", body = "hi" });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task EmptyRecipient_Returns400()
    {
        await _app.FlushRedisAsync();
        var response = await PostAsync(new { channel = "email", recipient = "", body = "hi" });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task EmptyBody_Returns400()
    {
        await _app.FlushRedisAsync();
        var response = await PostAsync(new { channel = "email", recipient = "x@x.com", body = "" });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ValidRequest_Returns202()
    {
        await _app.FlushRedisAsync();
        var response = await PostAsync(new { channel = "email", recipient = "x@x.com", body = "hi" });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Theory]
    [InlineData("not-a-url")]
    [InlineData("/relative/path")]
    [InlineData("ftp://example.com/hook")]
    public async Task WebhookWithInvalidUrl_Returns400(string recipient)
    {
        await _app.FlushRedisAsync();
        var response = await PostAsync(new { channel = "webhook", recipient, body = "{}" });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task WebhookWithValidHttpsUrl_Returns202()
    {
        await _app.FlushRedisAsync();
        var response = await PostAsync(new { channel = "webhook", recipient = "https://example.com/hook", body = "{}" });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }
}
