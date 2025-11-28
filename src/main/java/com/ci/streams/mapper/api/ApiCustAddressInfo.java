package com.ci.streams.mapper.api;

import static com.ci.streams.util.Env.must;

import com.ci.streams.avro.FailRecord;
import com.ci.streams.mapper.FieldMappingRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiCustAddressInfo implements ApiMapper {

  private static final Logger log = LoggerFactory.getLogger(ApiCustAddressInfo.class);

  private static final String API_ENDPOINT = "https://business.juso.go.kr/addrlink/addrLinkApi.do";

  private final String apiKey;

  private final List<String> inputFields;

  private final Schema apiSchema;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final Pattern WHITESPACE_RE = Pattern.compile("\\s+");
  private static final Pattern TRAILING_PUNCT_RE = Pattern.compile("[\\s,()\\-_/\\]\\[]+$");

  private static class MappingInput {
    final String originalAddress;
    final SearchResult searchResult;
    final ConsensusResult consensusResult;

    MappingInput(String originalAddress, SearchResult searchResult) {
      this.originalAddress = originalAddress;
      this.searchResult = searchResult;
      this.consensusResult =
          searchResult.getCandidates().size() > 1
              ? getConsensus(searchResult.getCandidates())
              : null;
    }
  }

  private final List<FieldMappingRule<MappingInput>> MAPPING_RULES;

  public ApiCustAddressInfo(Map<String, Object> params, Schema apiSchema) {

    this.apiKey = must("JUSO_API_KEY");

    this.inputFields = (List<String>) params.get("inputFields");

    this.apiSchema = apiSchema;
    this.httpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    if (log.isDebugEnabled()) {
      log.debug("JUSO_API_KEY loaded: {}", apiKey);
    }

    this.MAPPING_RULES =
        List.of(
            new FieldMappingRule<>("full_address", input -> input.originalAddress),
            new FieldMappingRule<>(
                "juso_matched_keyword", input -> input.searchResult.getUsedKeyword()),
            new FieldMappingRule<>(
                "juso_result_count", input -> input.searchResult.getCandidates().size()),
            new FieldMappingRule<>(
                "juso_quality",
                input -> {
                  if (input.searchResult.getCandidates().isEmpty()) return "not_found";
                  if (input.searchResult.getCandidates().size() == 1) return "ok";
                  if (input.consensusResult != null && input.consensusResult.isTripletConsensus())
                    return "ok";
                  return "ambiguous";
                }),
            new FieldMappingRule<>(
                "road_address",
                input -> {
                  if (input.searchResult.getCandidates().size() == 1) {
                    return input.searchResult.getCandidates().get(0).path("roadAddr").asText(null);
                  }
                  if (input.consensusResult != null && input.consensusResult.isFullConsensus()) {
                    return input.consensusResult.getRoadAddress();
                  }
                  return null;
                }),
            new FieldMappingRule<>(
                "zipcode",
                input -> {
                  if (input.searchResult.getCandidates().size() == 1) {
                    return input.searchResult.getCandidates().get(0).path("zipNo").asText(null);
                  }
                  if (input.consensusResult != null && input.consensusResult.isFullConsensus()) {
                    return input.consensusResult.getZipcode();
                  }
                  return null;
                }),
            new FieldMappingRule<>(
                "city",
                input -> {
                  if (input.searchResult.getCandidates().size() == 1) {
                    return input.searchResult.getCandidates().get(0).path("siNm").asText(null);
                  }
                  if (input.consensusResult != null && input.consensusResult.isTripletConsensus()) {
                    return input.consensusResult.getCity();
                  }
                  return null;
                }),
            new FieldMappingRule<>(
                "district",
                input -> {
                  if (input.searchResult.getCandidates().size() == 1) {
                    return input.searchResult.getCandidates().get(0).path("sggNm").asText(null);
                  }
                  if (input.consensusResult != null && input.consensusResult.isTripletConsensus()) {
                    return input.consensusResult.getDistrict();
                  }
                  return null;
                }),
            new FieldMappingRule<>(
                "town",
                input -> {
                  if (input.searchResult.getCandidates().size() == 1) {
                    return input.searchResult.getCandidates().get(0).path("emdNm").asText(null);
                  }
                  if (input.consensusResult != null && input.consensusResult.isTripletConsensus()) {
                    return input.consensusResult.getTown();
                  }
                  return null;
                }));
  }

  @Override
  public Record<String, GenericRecord> process(Record<String, GenericRecord> record) {
    GenericRecord inputRecord = record.value();
    if (inputRecord == null) {
      return null;
    }

    if (inputRecord.getSchema().getField("__op") != null && "d".equals(inputRecord.get("__op"))) {
      return null;
    }

    String keyword =
        inputFields.stream()
            .map(inputRecord::get)
            .filter(Objects::nonNull)
            .map(Object::toString)
            .collect(Collectors.joining(" "))
            .trim();

    if (keyword.isEmpty()) {
      if (log.isWarnEnabled()) {
        log.warn(
            "Input address fields are null or empty for record key: {}. Skipping API call.",
            record.key());
      }
      return null;
    }

    try {
      SearchResult searchResult = searchAddress(keyword);
      GenericRecord outputRecord = createOutputRecord(keyword, searchResult);

      if (inputRecord.getSchema().getField("__op") != null && inputRecord.get("__op") != null) {
        new FieldMappingRule<GenericRecord>("__op", input -> input.get("__op"))
            .apply(inputRecord, outputRecord);
      }

      return record.withKey(keyword).withValue(outputRecord);
    } catch (Exception e) {
      if (e instanceof IOException && e.getMessage().contains("E0006")) {
        if (log.isWarnEnabled()) {
          log.warn("Skipping record due to JUSO API error E0006: {}", e.getMessage());
        }
        return null;
      }
      if (log.isErrorEnabled()) {
        log.error("Error processing record: {}", record.key(), e);
      }
      FailRecord failRecord =
          FailRecord.newBuilder()
              .setPayload(record.value().toString())
              .setReason(e.getMessage())
              .setFailedAt(Instant.now())
              .build();
      return record.withValue(failRecord);
    }
  }

  private GenericRecord createOutputRecord(String inputAddress, SearchResult searchResult) {
    GenericRecord outputRecord = new GenericData.Record(apiSchema);
    MappingInput mappingInput = new MappingInput(inputAddress, searchResult);

    for (FieldMappingRule<MappingInput> rule : MAPPING_RULES) {
      rule.apply(mappingInput, outputRecord);
    }

    return outputRecord;
  }

  private SearchResult searchAddress(String keyword) throws IOException, InterruptedException {
    String tempKeyword =
        TRAILING_PUNCT_RE
            .matcher(WHITESPACE_RE.matcher(keyword).replaceAll(" ").trim())
            .replaceAll("");
    String originalKeyword = tempKeyword;

    while (!tempKeyword.isEmpty()) {
      try {
        log.debug("Searching address with keyword: '{}'", tempKeyword);
        JsonNode result = callJusoApi(tempKeyword);
        List<JsonNode> candidates = toList(result.path("results").path("juso"));
        if (!candidates.isEmpty()) {
          return new SearchResult(tempKeyword, candidates);
        }
      } catch (IOException e) {
        if (!e.getMessage().contains("E0006")) {
          throw e;
        }
        log.warn(
            "JUSO API error E0006 for keyword '{}'. Retrying with a shorter keyword.", tempKeyword);
      }

      if (tempKeyword.contains(" ")) {
        tempKeyword = tempKeyword.substring(0, tempKeyword.lastIndexOf(' ')).trim();
      } else {
        break;
      }
    }

    return new SearchResult(originalKeyword, new ArrayList<>());
  }

  private JsonNode callJusoApi(String keyword) throws IOException, InterruptedException {
    String encodedKeyword = URLEncoder.encode(keyword, StandardCharsets.UTF_8);
    String url =
        String.format(
            "%s?confmKey=%s&keyword=%s&resultType=json&countPerPage=20",
            API_ENDPOINT, apiKey, encodedKeyword);

    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(url)).header("Accept", "application/json").build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException(
          "API call failed with status code: "
              + response.statusCode()
              + " and body: "
              + response.body());
    }

    JsonNode responseNode = objectMapper.readTree(response.body());
    JsonNode common = responseNode.path("results").path("common");
    String errorCode = common.path("errorCode").asText("0");
    if (!"0".equals(errorCode)) {
      throw new IOException(
          "JUSO API error " + errorCode + ": " + common.path("errorMessage").asText());
    }

    return responseNode;
  }

  private static ConsensusResult getConsensus(List<JsonNode> candidates) {
    String city = getConsensusValue(candidates, "siNm");
    String district = getConsensusValue(candidates, "sggNm");
    String town = getConsensusValue(candidates, "emdNm");
    String zipcode = getConsensusValue(candidates, "zipNo");
    String roadAddress = getConsensusValue(candidates, "roadAddr");

    return new ConsensusResult(city, district, town, zipcode, roadAddress);
  }

  private static String getConsensusValue(List<JsonNode> candidates, String fieldName) {
    return candidates.stream()
                .map(c -> c.path(fieldName).asText(null))
                .filter(Objects::nonNull)
                .map(String::trim)
                .distinct()
                .count()
            == 1
        ? candidates.get(0).path(fieldName).asText().trim()
        : null;
  }

  private List<JsonNode> toList(JsonNode node) {
    if (node.isMissingNode() || !node.isArray()) {
      return new ArrayList<>();
    }
    return StreamSupport.stream(node.spliterator(), false).collect(Collectors.toList());
  }

  private static class SearchResult {
    private final String usedKeyword;
    private final List<JsonNode> candidates;

    public SearchResult(String usedKeyword, List<JsonNode> candidates) {
      this.usedKeyword = usedKeyword;
      this.candidates = candidates;
    }

    public String getUsedKeyword() {
      return usedKeyword;
    }

    public List<JsonNode> getCandidates() {
      return candidates;
    }
  }

  private static class ConsensusResult {
    private final String city;
    private final String district;
    private final String town;
    private final String zipcode;
    private final String roadAddress;

    public ConsensusResult(
        String city, String district, String town, String zipcode, String roadAddress) {
      this.city = city;
      this.district = district;
      this.town = town;
      this.zipcode = zipcode;
      this.roadAddress = roadAddress;
    }

    public boolean isTripletConsensus() {
      return city != null && district != null && town != null;
    }

    public boolean isFullConsensus() {
      return isTripletConsensus() && zipcode != null && roadAddress != null;
    }

    public String getCity() {
      return city;
    }

    public String getDistrict() {
      return district;
    }

    public String getTown() {
      return town;
    }

    public String getZipcode() {
      return zipcode;
    }

    public String getRoadAddress() {
      return roadAddress;
    }
  }
}
