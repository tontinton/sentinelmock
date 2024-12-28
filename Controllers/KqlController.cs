using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Nodes;
using KustoLoco.Core;
using Microsoft.AspNetCore.Mvc;

namespace sentinelmock.Controllers {

public record BatchRequest(List<RequestItem> requests);
public record RequestItem(string id, RequestBody body);
public record RequestBody(string query);

public record BatchResponses(List<BatchResponse> responses);
public record BatchResponse(string id, int status, BodyResponse? body);
public record BodyResponse(List<TablesResponse> tables);
public record TablesResponse(string name, List<ColumnResponse> columns,
                             List<object?[]> rows);
public record ColumnResponse(string name, string type);

[ApiController]
[Route("")]
public class KqlController : ControllerBase {
  private readonly ILogger<KqlController> logger;
  private readonly KustoQueryContext context;

  public KqlController(ILogger<KqlController> logger,
                       KustoQueryContext context) {
    this.logger = logger;
    this.context = context;
  }

  private static string GetKqlType(Type type) {
    if (type == null)
      throw new ArgumentNullException(nameof(type));

    Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;

    return underlyingType switch {
      Type t when t == typeof(string) => "string",
      Type t when t == typeof(int) => "int",
      Type t when t == typeof(long) => "long",
      Type t when t == typeof(double) => "real",
      Type t when t == typeof(decimal) => "decimal",
      Type t when t == typeof(DateTime) => "datetime",
      Type t when t == typeof(TimeSpan) => "timespan",
      Type t when t == typeof(bool) => "bool",
      Type t when t == typeof(Guid) => "guid",
      Type t when t.IsArray => "dynamic",
      Type t when typeof(System.Collections.IEnumerable).IsAssignableFrom(t) =>
          "dynamic",
      Type t when t == typeof(JsonNode) => "dynamic",
      _ => throw new ArgumentException($"Unsupported type: {type.FullName}")
    };
  }

  private static object? ParseJsonElement(JsonElement element) {
    switch (element.ValueKind) {
    case JsonValueKind.String:
      return element.GetString();

    case JsonValueKind.Number:
      if (element.TryGetInt32(out int intValue)) {
        return intValue;
      } else if (element.TryGetDouble(out double doubleValue)) {
        return doubleValue;
      }
      throw new ArgumentException($"Unsupported number type");

    case JsonValueKind.True:
      return true;

    case JsonValueKind.False:
      return false;

    case JsonValueKind.Null:
      return null;

    case JsonValueKind.Object:
      return JsonNode.Parse(element.GetRawText());

    case JsonValueKind.Array:
      return JsonNode.Parse(element.GetRawText());
    }

    throw new ArgumentException($"Unsupported type: {element.ValueKind}");
  }

  [HttpPost("table/{table}")]
  public void CreateTable([FromRoute] string table,
                          [FromBody] List<JsonElement> records) {
    if (records.Count == 0) {
      return;
    }

    logger.LogInformation("Creating table '{}' with {} number of rows", table,
                          records.Count);

    var builder = TableBuilder.CreateEmpty(table, records.Count);
    foreach (var item in records.First().EnumerateObject()) {
      var type = ParseJsonElement(item.Value)!.GetType();
      builder.WithColumn(
          item.Name, type,
          records
              .Select(r => {
                if (r.TryGetProperty(item.Name, out JsonElement value)) {
                  return ParseJsonElement(value);
                } else {
                  throw new ArgumentException(
                      $"Not all records contain ${item.Name}");
                }
              })
              .ToImmutableArray());
    }
    context.AddTable(builder);
  }

  [HttpPost("v1/$batch")]
  public async Task<BatchResponses>
  BatchQuery([FromBody] BatchRequest batchRequest) {
    var tasks = batchRequest.requests.Select(async request => {
      var query = request.body.query;
      logger.LogInformation("Running '{}'", query);

      try {
        var result = await context.RunQuery(query);

        if (result.Error != "") {
          logger.LogError("Failed running query '{}': {}", query, result.Error);
          return new BatchResponse(request.id, 400, null);
        }

        logger.LogInformation("Success running query '{}': {} rows", query,
                              result.RowCount);

        var columns = result.ColumnDefinitions()
                          .Select(c => new ColumnResponse(
                                      c.Name, GetKqlType(c.UnderlyingType)))
                          .ToList();

        var table = new TablesResponse("PrimaryResult", columns,
                                       result.EnumerateRows().ToList());

        var response = new BodyResponse(new List<TablesResponse> { table });
        return new BatchResponse(request.id, 200, response);
      } catch (Exception ex) {
        logger.LogError(ex, "Exception running query '{}': {}", query,
                        ex.ToString());
        return new BatchResponse(request.id, 500, null);
      }
    });

    var responses = await Task.WhenAll(tasks);
    return new BatchResponses(responses.ToList());
  }
}
}
