import pytest
import pandas as pd
from motor_learning_network.get_references import dois_to_query__with_loaded_references

@pytest.fixture
def sample_dois(self):
    return pd.Series([
    '10.1016/s0896-6273(03)00562-2', 
    '10.1152/jn.00390.2018', 
    '10.1053/j.semperi.2016.08.010', 
    '10.1136/bmjopen-2012-000824', 
    '10.1007/s00464-025-11825-y', 
])

@pytest.fixture
def loaded_references(self):
    return pd.Series([
    '10.1016/s0896-6273(03)00562-2', 
    '10.1152/jn.00390.2018', 
])

@pytest.mark.unit
class TestDoisToQueryWithLoadedReferences:
    """Tests for the dois_to_query__with_loaded_references function."""
    
    def test_returns_dois_not_in_loaded_references(self, sample_dois, loaded_references):
        """Test that only DOIs NOT in loaded_references are returned."""
        # Act
        result = dois_to_query__with_loaded_references(sample_dois, loaded_references)
        
        # Assert
        expected_dois = {'10.1000/abc456'}
        assert set(result.values) == expected_dois
        assert len(result) == 1
    
    def test_returns_all_when_no_overlap(self):
        """Test that all DOIs are returned when none are in loaded_references."""
        # Arrange
        cleaned_dois = pd.Series(['10.1000/xyz123', '10.1000/abc456'])
        loaded_references = pd.Series(['10.1000/def789', '10.1000/ghi000'])
        
        # Act
        result = dois_to_query__with_loaded_references(cleaned_dois, loaded_references)
        
        # Assert
        assert len(result) == 2
        assert set(result.values) == {'10.1000/xyz123', '10.1000/abc456'}
    
    def test_returns_empty_when_complete_overlap(self):
        """Test that empty Series is returned when all DOIs are in loaded_references."""
        # Arrange
        cleaned_dois = pd.Series(['10.1000/xyz123', '10.1000/abc456'])
        loaded_references = pd.Series(['10.1000/xyz123', '10.1000/abc456'])
        
        # Act
        result = dois_to_query__with_loaded_references(cleaned_dois, loaded_references)
        
        # Assert
        assert len(result) == 0