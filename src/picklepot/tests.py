import pytest
import pathlib
import os
from pathlib import Path
from picklepot import PicklePot

@pytest.fixture
def pickle_pot(tmp_path):
    return PicklePot(pickle_dir=tmp_path, pot_name='pp', print_info=False)

def test_dump_and_load(pickle_pot, tmp_path):
    obj = 'test_object'
    obj_name = 'test'
    pickle_pot.dump(obj, obj_name)
    loaded_obj = pickle_pot.load_obj(obj_name)
    assert loaded_obj == obj

def test_versions(pickle_pot, tmp_path):
    obj = 'test_object'
    obj_name = 'test'
    pickle_pot.dump(obj, obj_name)
    versions = pickle_pot.versions()
    assert versions[obj_name] == 1

def test_ledger(pickle_pot, tmp_path):
    obj = 'test_object'
    obj_name = 'test'
    pickle_pot.dump(obj, obj_name, info='test_info')
    ledger = pickle_pot.ledger()
    print(ledger)
    assert ledger['Name'].iloc[0] == obj_name
    assert ledger['Version'].iloc[0] == 1
    assert ledger['Info'].iloc[0] == 'test_info'


def test_initial(tmp_path):
    # tmp_path = pathlib.Path(tmp_path)
    # try:
    #     os.rmdir(tmp_path)
    # except:
    #     pass
    i0 = 'xxx'
    i1 = 'aaa'
    i2 = 'fff'
    p = PicklePot(tmp_path, 'fart')
    p.dump(i0, 'o1', 'v0')
    p.dump(i1, 'o1', 'v1')
    p.dump(i1, 'o1', 'v1 overwrit', overwrite_latest=True)
    p.dump(i2, 'o2')
    p2 = PicklePot(tmp_path, pot_name='fart2')

    # can load specific ver
    assert  i0 == p.load_obj('o1', version=1)
    # auto load objects match
    assert p2['o1'] == i1
    assert p2['o2'] == i2

    print('ledger:', p.ledger())
    print('ledger, latest only', p.ledger(latest_only=True))
    print('ver history:', p.print_version_history('o1', print_it=True))
    # import glob
    # for fn in  glob.glob(str(tmp_path / '*.pickle')):
    #     os.remove(fn)
    # os.remove(tmp_path / p._ledger_path)
    #
    # os.rmdir(tmp_path)

if __name__ == '__main__':
    p = Path('/home/jcthomas/tmp/picklepot')
    # test_initial()
    # test_dump_and_load()
    # test_versions()
    # test_initial()