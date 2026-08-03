use haze_location::contract::{QueryRequest, QueryResponse};

#[test]
fn shared_query_fixture_preserves_zero_coordinates() {
    let raw = include_str!("../../../contracts/location/v1/query.json");
    let request: QueryRequest = serde_json::from_str(raw).expect("query fixture must deserialize");
    let input = request.input.expect("fixture has one input");
    match input {
        haze_location::contract::LocationInput::Point {
            latitude,
            longitude,
        } => {
            assert_eq!(latitude, 0.0);
            assert_eq!(longitude, 0.0);
        }
        other => panic!("expected point input, got {other:?}"),
    }
}

#[test]
fn shared_response_fixture_matches_v1_contract() {
    let raw = include_str!("../../../contracts/location/v1/response.json");
    let response: QueryResponse =
        serde_json::from_str(raw).expect("response fixture must deserialize");
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].entity.deployments.len(), 1);
    assert_eq!(
        response.results[0].grouping.as_ref().unwrap().member_count,
        1
    );
}
