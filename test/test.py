from test_flunet.validation import flunet_validation, flunet_count


def test_flunet_count():
    assert flunet_count() == True


def test_flunet_accuracy():
    assert flunet_validation() == 0
