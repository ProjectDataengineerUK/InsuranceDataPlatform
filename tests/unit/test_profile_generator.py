from src.open_insurance.profile_generator import generate_profile


def test_generate_profile_is_deterministic():
    first = generate_profile("policy-123")
    second = generate_profile("policy-123")
    assert first == second


def test_generate_profile_differs_across_policy_ids():
    a = generate_profile("policy-a")
    b = generate_profile("policy-b")
    assert a != b


def test_generate_profile_values_within_bounds():
    profile = generate_profile("policy-xyz")
    assert 18 <= profile["synthetic_age"] <= 80
    assert 0.0 <= profile["risk_score"] <= 1.0
    assert profile["policy_id"] == "policy-xyz"
