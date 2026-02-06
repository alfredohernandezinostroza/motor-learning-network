import pytest
import pandas as pd
from motor_learning_network.get_references import dois_to_query__with_loaded_references

@pytest.fixture
def sample_dois():
    return pd.Series([
    '10.1016/s0896-6273(03)00562-2',  #1
    '10.1152/jn.00390.2018',  #2
    '10.1053/j.semperi.2016.08.010',  #3
    '10.1136/bmjopen-2012-000824',  #4
    '10.1007/s00464-025-11825-y',  #5
])

@pytest.fixture
def loaded_references():
    return pd.Series([
    '10.1016/s0896-6273(03)00562-2', #1
    '10.1152/jn.00390.2018', #2
])
########################
###### UNIT TESTS ######
########################
@pytest.mark.unit
def test_returns_dois_not_in_loaded_references(sample_dois, loaded_references):
    """Test that only DOIs NOT in loaded_references are returned."""
    # Act
    result = dois_to_query__with_loaded_references(sample_dois, loaded_references)
    print(result)
    # Assert
    expected_dois = set(sample_dois) - set(loaded_references)
    assert set(result.values) == expected_dois
    assert len(result) == 3

@pytest.mark.unit
def test_returns_all_when_no_overlap(sample_dois):
    """Test that all DOIs are returned when none are in loaded_references."""
    # Act
    result = dois_to_query__with_loaded_references(sample_dois, loaded_references=['doi.example.not.in.sample_dois'])
    
    # Assert
    assert len(result) == 5
    assert set(result.values) == set(sample_dois)

@pytest.mark.unit
def test_returns_empty_when_complete_overlap(sample_dois):
    """Test that empty Series is returned when all DOIs are in loaded_references."""
    
    # Act
    result = dois_to_query__with_loaded_references(sample_dois, loaded_references=sample_dois)
    
    # Assert
    assert len(result) == 0
    
###############################
###### INTEGRATING TESTS ######
###############################

###########################
###### END2END TESTS ######
###########################