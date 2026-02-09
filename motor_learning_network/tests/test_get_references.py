import pytest
import pandas as pd
from motor_learning_network.get_references import dois_to_query__with_loaded_references, fetched_references
import pprint


@pytest.fixture
def five_reference_lists() -> dict[str,list[str]]:
    return {
        '10.1016/s0896-6273(03)00562-2': ['10.1016/S0896-6273(00)80808-9',
                                   '10.1016/0025-5564(71)90051-4',
                                   '10.1097/00001756-200108080-00014',
                                   '10.1097/00001756-199010000-00013',
                                   '10.1016/0006-8993(91)90867-U',
                                   '10.1016/S0896-6273(00)00070-2',
                                   '10.1016/0168-0102(94)90145-7',
                                   '10.1016/0006-8993(72)90110-2',
                                   '10.1146/annurev.ne.05.030182.001423',
                                   '10.2183/pjab1945.50.85',
                                   '10.1016/0042-6989(80)90128-5',
                                   '10.1073/pnas.122206399',
                                   '10.1152/jn.1995.74.1.489',
                                   '10.1016/0896-6273(91)90076-C',
                                   '10.1111/j.1749-6632.1996.tb15725.x',
                                   '10.1152/jn.1978.41.3.733',
                                   '10.1152/jn.1994.72.2.954',
                                   '10.1152/jn.1994.72.2.928',
                                   '10.1007/BF00233976',
                                   '10.1113/jphysiol.1969.sp008820',
                                   '10.1152/jn.1998.79.3.1286',
                                   '10.1007/BF00243305',
                                   '10.1152/jn.1980.43.5.1406',
                                   '10.1016/0006-8993(74)91035-X',
                                   '10.1152/jn.1980.43.5.1477',
                                   '10.1101/lm.1.1.1',
                                   '10.1016/S0896-6273(02)00606-2',
                                   '10.1523/JNEUROSCI.19-17-07326.1999',
                                   '10.1016/S0896-6273(02)01064-4',
                                   '10.1152/jn.1995.73.2.615',
                                   '10.1152/jn.1995.73.2.632',
                                   '10.1152/jn.1994.72.3.1383',
                                   '10.1111/j.1469-7793.2000.00563.x',
                                   '10.1152/jn.1976.39.5.954',
                                   '10.1016/S0896-6273(00)80099-9',
                                   '10.1523/JNEUROSCI.19-13-05683.1999',
                                   '10.1016/0022-5193(77)90146-1',
                                   '10.1038/349326a0',
                                   '10.1152/jn.00821.2001',
                                   '10.1016/0959-4388(95)80023-9',
                                   '10.1523/JNEUROSCI.16-02-00853.1996',
                                   '10.1016/S0896-6273(01)00342-7',
                                   '10.1016/S0006-8993(00)03180-2',
                                   '10.1073/pnas.130414597'],
 '10.1152/jn.00390.2018': ['10.1126/science.1155140',
                           '10.1016/j.cub.2009.01.036',
                           '10.1016/j.bbr.2009.08.031',
                           '10.1371/journal.pcbi.1005503',
                           '10.1146/annurev-neuro-072116-031548',
                           '10.1523/JNEUROSCI.5406-09.2010',
                           '10.1038/nrn2258',
                           '10.1152/jn.01311.2006',
                           '10.1523/JNEUROSCI.1294-12.2013',
                           '10.7554/eLife.03697',
                           '10.1037/0097-7403.27.2.165',
                           '10.1037/0097-7403.30.4.271',
                           '10.1152/jn.00901.2017',
                           '10.1016/j.neuron.2011.04.012',
                           '10.1371/journal.pcbi.1002012',
                           '10.1152/jn.2002.88.3.1533',
                           '10.1523/JNEUROSCI.4218-04.2005',
                           '10.1016/j.conb.2011.06.012',
                           '10.1152/jn.01055.2015',
                           '10.1523/JNEUROSCI.2733-16.2016',
                           '10.1152/jn.00734.2013',
                           '10.1523/JNEUROSCI.3303-16.2017',
                           '10.1523/JNEUROSCI.1046-15.2015',
                           '10.1152/jn.00673.2014',
                           '10.1523/JNEUROSCI.3244-14.2015',
                           '10.1152/jn.00965.2014',
                           '10.1523/JNEUROSCI.1767-16.2016',
                           '10.1016/j.lmot.2011.05.001',
                           '10.1037/a0015971',
                           '10.1109/TNN.1998.712192',
                           '10.1007/s00221-001-0928-1',
                           '10.1093/brain/awv329',
                           '10.1038/nature06390',
                           '10.1093/cercor/bhx214',
                           '10.1152/jn.00652.2003',
                           '10.1523/JNEUROSCI.6525-10.2011',
                           '10.1038/nn.3616',
                           '10.1152/jn.90529.2008'],
 '10.1053/j.semperi.2016.08.010': ['10.1016/j.siny.2013.08.010',
                                   '10.21236/ADA300953',
                                   '10.1097/SIH.0b013e31829ac85c',
                                   '10.1038/jp.2016.42',
                                   '10.1542/peds.106.4.e45',
                                   '10.1542/neo.9-4-e142',
                                   '10.1097/PCC.0b013e3182417709',
                                   '10.1016/j.jpeds.2007.06.012',
                                   '10.1016/j.jsurg.2015.01.020',
                                   '10.1186/s12909-015-0389-z',
                                   '10.3109/13561820.2015.1017555',
                                   '10.1016/j.jsurg.2015.03.005',
                                   '10.1016/j.jsurg.2015.06.012',
                                   '10.3109/13561820.2014.982789',
                                   '10.1542/peds.2013-3267',
                                   '10.1542/peds.2005-0320',
                                   '10.1097/01.SIH.0000243550.24391.ce',
                                   '10.1097/01.SIH.0000243551.01521.74',
                                   '10.1136/bmjqs-2012-001336',
                                   '10.1016/j.resuscitation.2012.07.035',
                                   '10.1016/j.resuscitation.2014.12.016',
                                   '10.1097/SIH.0000000000000011',
                                   '10.1053/j.semperi.2011.01.007',
                                   '10.1016/j.siny.2008.04.015',
                                   '10.1542/peds.2011-0657',
                                   '10.1203/PDR.0b013e3181c2def3',
                                   '10.1203/PDR.0b013e3181c910c8',
                                   '10.1053/j.semperi.2011.01.006'],
 '10.1136/bmjopen-2012-000824': ['10.1097/00004583-199908000-00018',
                                 '10.1007/BF00794611',
                                 '10.1017/S2045796011000801',
                                 '10.1016/j.jpsychires.2005.10.004',
                                 '10.1097/00004583-199401000-00012',
                                 '10.1016/j.puhe.2010.12.008',
                                 '10.1016/j.jad.2010.11.023',
                                 '10.1034/j.1600-0447.2000.00011.x',
                                 '10.1093/med:psych/9780195111606.003.0004',
                                 '10.1001/jama.2010.1456',
                                 '10.1002/14651858.CD007171.pub2',
                                 '10.4324/9781410612847',
                                 '10.1177/107319110000700203',
                                 '10.1177/1073191106291815',
                                 '10.1111/j.1440-1819.2007.01688.x',
                                 '10.1007/s12199-011-0225-y',
                                 '10.1111/j.1442-200X.2007.02430.x',
                                 '10.1093/pubmed/fdi031',
                                 '10.1016/0895-4356(95)00556-0',
                                 '10.1016/j.jenvman.2011.06.003',
                                 '10.1016/j.jadohealth.2010.09.012',
                                 '10.1136/jech.2006.042069'],
 '10.1007/s00464-025-11825-y': ['10.1007/s00464-019-07265-0',
                                '10.1001/jamasurg.2015.4442',
                                '10.1007/s00464-022-09379-4',
                                '10.1016/j.jsurg.2017.09.027',
                                '10.1016/j.jsurg.2016.04.024',
                                '10.1016/j.jsurg.2021.11.012',
                                '10.1007/s00464-021-08375-4',
                                '10.1177/1553350619891379',
                                '10.1097/SLA.0000000000003510',
                                '10.1097/SLA.0000000000001652',
                                '10.1093/bjsopen/zrad063',
                                '10.1097/01.sla.0000118749.24645.45',
                                '10.1177/1553350619853099',
                                '10.1016/j.jviscsurg.2021.01.004',
                                '10.1177/15533506221106258',
                                '10.1097/AOG.0000000000005677',
                                '10.1001/jamasurg.2016.4619',
                                '10.1097/XCS.0000000000000217',
                                '10.1007/s00464-017-5690-y',
                                '10.1097/SLA.0000000000001214',
                                '10.1097/SLA.0000000000000857',
                                '10.1007/s00464-016-5366-z',
                                '10.1007/s11701-016-0572-1',
                                '10.1016/j.amjsurg.2005.04.004',
                                '10.1016/j.jsurg.2020.07.011',
                                '10.1007/s00464-023-10297-2',
                                '10.1007/s00464-022-09509-y',
                                '10.1207/S15328015TLM1504_11',
                                '10.1097/ACM.0000000000000853',
                                '10.1007/s00464-019-07089-y',
                                '10.1007/s00464-020-07628-y',
                                '10.1007/s11192-023-04675-9',
                                '10.1371/journal.pone.0224697',
                                '10.1136/postgradmedj-2013-132486',
                                '10.1002/lary.27286',
                                '10.1001/jamasurg.2014.1779',
                                '10.1016/j.athoracsur.2014.09.051',
                                '10.1097/SLA.0000000000005804',
                                '10.1007/s00464-023-10311-7']
                                }

@pytest.fixture
def five_dois(five_reference_lists) -> pd.Series:
    return pd.Series(five_reference_lists.keys())

@pytest.fixture
def two_reference_lists(five_reference_lists) -> dict[str,list[str]]:
    return {k: five_reference_lists[k] for k in ('10.1007/s00464-025-11825-y','10.1136/bmjopen-2012-000824')}

########################
###### UNIT TESTS ######
########################

@pytest.mark.unit
def test_fetched_references(five_dois):
    """Test that the references function returns a dictionary with the correct structure."""
    # Act
    result = fetched_references(five_dois)
    
    # Assert
    assert isinstance(result, dict)
    for key, value in result.items():
        assert isinstance(key, str)  # DOI should be a string
        assert isinstance(value, list)  # References should be a list
        for ref in value:
            assert isinstance(ref, str)  # Each reference DOI should be a string
    pprint.pp(result)

@pytest.mark.unit
def test_returns_dois_not_in_loaded_references(five_dois, two_reference_lists):
    """Test that only DOIs NOT in loaded_references are returned."""
    # Act
    result = dois_to_query__with_loaded_references(five_dois, loaded_references=two_reference_lists)
    print(result)
    # Assert
    expected_dois = set(five_dois) - set(two_reference_lists)
    assert set(result.values) == expected_dois
    assert len(result) == 3

@pytest.mark.unit
def test_returns_all_when_no_overlap(five_dois):
    """Test that all DOIs are returned when none are in loaded_references."""
    # Act
    result = dois_to_query__with_loaded_references(five_dois, loaded_references={'doi.example.not.in.sample_dois': ["example.reference"]})
    
    # Assert
    assert len(result) == 5
    assert set(result.values) == set(five_dois)

@pytest.mark.unit
def test_returns_empty_when_complete_overlap(five_dois, five_reference_lists):
    """Test that empty Series is returned when all DOIs are in loaded_references."""
    
    # Act
    result = dois_to_query__with_loaded_references(five_dois, loaded_references=five_reference_lists)
    
    # Assert
    assert len(result) == 0


###############################
###### INTEGRATING TESTS ######
###############################

###########################
###### END2END TESTS ######
###########################
