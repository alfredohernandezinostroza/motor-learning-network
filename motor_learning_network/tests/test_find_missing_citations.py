import pytest
import pandas as pd
from motor_learning_network.find_missing_citations import missing_citations_openalex_ids

@pytest.fixture
def dois():
    return pd.DataFrame({
        'citing_doi': ["10.4324/9781315059167-5", "10.4324/9780203628942-11","10.4108/icst.pervasivehealth.2014.255323"],
        'cited_dois': [[],[],[]]
        })

def test_missing_citations(dois):
    result, errors = missing_citations_openalex_ids(dois)
    assert set(result['10.4108/icst.pervasivehealth.2014.255323']) == set(["W82719784","W1605287056","W2107911338","W2784030868"])
    assert len(result) == 1
    assert errors["10.4324/9781315059167-5"] == 'referenced_works is empty'
    assert errors["10.4324/9780203628942-11"] == 'referenced_works is empty'
    assert len(errors) == 2