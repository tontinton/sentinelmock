using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Nodes;
using KustoLoco.Core;
using Microsoft.AspNetCore.Mvc;

namespace sentinelmock.Controllers {
[ApiController]
[Route("[controller]")]
public class KqlController : ControllerBase {
  private readonly ILogger<KqlController> _logger;
  private readonly KustoQueryContext context;

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

  public KqlController(ILogger<KqlController> logger) {
    _logger = logger;
    context = new KustoQueryContext();
  }

  [HttpPost("table/{table}")]
  public async Task CreateTable([FromRoute] string table,
                                [FromBody] List<JsonElement> records) {
    if (records.Count == 0) {
      return;
    }

    _logger.LogInformation("Creating table '{}' with {} number of rows", table,
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

    var result = await context.RunQuery(table);
    _logger.LogInformation(
        "Result {}: {}, {}", result.Error, result.ColumnCount,
        JsonSerializer.Serialize(result.EnumerateRows().ToImmutableArray()));
  }
}
}
