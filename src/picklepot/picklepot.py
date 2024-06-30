import os, pickle, logging
import pathlib
import datetime

logging.basicConfig()
logger = logging.getLogger(__name__)

try:
    import pandas as pd
except ImportError:
    logger.info('Pandas is not installed, ledger will loaded as a string.')

#todo add hashing.

# pandas.util.hash_pandas_object returns a series same length as the original thing,
#    could probbably hash(sum(thing.map(str)))

#todo so what I could do is have every object in it's own directory with its
#  own history. So deleting that object entirely would be a case of deleting the dir...


def print_assign_strings(self, potname='picklepot'):
        # for k in self.objects:
        #     print(f"{k} = {instance_name}.objects['{k}']")
    s = f"""
# to move picklepot objects to global...
exclude = []
for k in {potname}.objects:
    if k not in exclude:
        exec(f"{{k}} = {potname}.objects['{{k}}']")
"""
    print(s)

class PicklePot:
    """Pickle objects, quickily load latest versions, previous versions are kept,
    not overwritten.
    """

    def __init__(self, pickle_dir='pp',
                 exclude:list=None, include_only:list=None, print_info=True):
        """
        If a

        Parameters:
            pickle_dir (str): The directory path to load and dump pickled objects.

        """

        if exclude and include_only:
            logger.warning("It doesn't make sense to set non-null values for both exclude and include_only, "
                           "exclude will be ignored.")
        if exclude is None:
            exclude = []
        self.exclude = exclude
        self.include_only = include_only
        os.makedirs(pickle_dir, exist_ok=True)
        self.pickle_dir = pickle_dir
        self.objects = {}
        self._ledger_path = pathlib.Path(self.pickle_dir) / f"picklepot_ledger.tsv"
        self.load_latest_objects()
        if print_info:
            try:
                self.print_object_info()
            except FileNotFoundError:
                print('No ledger file found. One will be created when an object is dumped.')

        self.print_object_info()

    def _read_ledger(self) -> pd.DataFrame:
        ledger = pd.read_csv(self._ledger_path, sep='\t', header=None)
        ledger.columns = ['Name', 'Version', 'Info', 'Date']
        # todo datatypes, and can pass columns in read_csv
        return ledger


    def print_latest_pickles(self):
        latest = self.versions()
        for k, v in latest.items():
            print(f"{k} - V{v}")

    def _write_to_ledger(self, objname, ver, info):
        ver = str(ver)

        with open(self._ledger_path, 'a') as f:
            d = datetime.datetime.now()
            now = f"{d.year}-{d.month}-{d.day} {d.hour}:{d.minute}"
            info = info.replace('\t', '    ')
            f.write(
                '\t'.join(
                    [objname, ver, info, now]
                ) +'\n'
            )

    def ledger(self, latest_only=False, included_only=False) -> pd.DataFrame:
        """A DataFrame of files written to the picklepot.

        Args:
            latest_only: When True, include only only the last written version for each
                object name.
            included_only: When True, respect self.included and self.excluded. """
        # todo seems to include excluded items.
        ledger = self._read_ledger()

        if included_only:
            m = ledger.index.map(self._is_included)
            ledger = ledger.loc[m]
        if latest_only:
            return ledger.loc[~ledger.Name.duplicated(keep='last')]
        # filter out duplicate entries resulting from overwrite_latest=True
        return ledger.loc[~ledger.duplicated(['Name', 'Version', 'Info',], keep='last')]

    def print_object_info(self, included_only=True):
        ledge = self.ledger(included_only=included_only)
        for name, idx in ledge.groupby('Name').groups.items():
            print(f"{name}: ")
            for _, row in ledge.loc[idx].iterrows():
                print(f"\tV{row.Version}, {row.Date},   {row.Info}")

    def version_history(self, objname, print_it=True) -> pd.DataFrame:
        ledger = self.ledger()
        ledger:pd.DataFrame = ledger.loc[ledger.Name == objname]
        if print_it:
            for _, row in ledger.iterrows():
                print(
                    f"{row.Version} ({row.Date}):  {row.Info}"
                )
        return ledger

    def versions(self, source_dir=None) -> dict[str, int]:
        """List available versions of pickled objects in source directory.
        Default source_dir is self.pickle_dir"""
        versions = {}
        if source_dir is None:
            source_dir = self.pickle_dir
        for file in os.listdir(source_dir):
            if file.endswith('.pickle'):
                obj_name, version = os.path.splitext(file)[0].split('.')
                version = int(version)
                if (obj_name not in versions) or (version > versions[obj_name]):
                    versions[obj_name] = version
        return versions

    def _is_included(self, obj_name):
        """"""
        #todo figure out how to prioritise this
        if self.include_only:
            if obj_name not in self.include_only:
                return False
        if obj_name in self.exclude:
            return False
        return True

    def load_latest_objects(self):
        """
        Load the latest version of pickles in PicklePot.pickle_dir to self.objects,
        respecting self.included & self.excluded.
        """

        for obj_name, version in self.versions().items():
            if not self._is_included(obj_name):
                continue

            self.objects[obj_name] = self.load_obj(obj_name, version)
        print('\n')


    def dump(self, obj, obj_name, info='', overwrite_latest=False):
        """
        Writes a pickled object to a file in the pickle_dir directory.
        If the object has already been pickled, the version number is incremented.

        Parameters:
            obj: A picklable object
            obj_name (str): The name AS STRING of the object to pickle.
            info: String describing the object, or differences from previous versions.
            overwrite_latest: don't incriment the version number, overwrite highest
                version pickle file.
        """
        versions = self.versions()
        if obj_name in versions:
            version = versions[obj_name]
            if not overwrite_latest:
                version += 1

            file_name = os.path.join(self.pickle_dir, f"{obj_name}.{version}.pickle")
        else:
            version = 1
            file_name = os.path.join(self.pickle_dir, f"{obj_name}.{version}.pickle")

        with open(file_name, 'wb') as f:
            pickle.dump(obj, f)
            self._write_to_ledger(obj_name, version, info)

        print(f"Saved {obj_name} (version {version})")


    def load_obj(self, obj_name, version:int='latest', source_dir=None):
        """
        Load a specific object. Use `version=n` to get a specific version.

        Parameters:
            obj_name (str): The name of the object to load.
            version (int): The version number of the object to load. 0 loads the latest version.
            source_dir: Source dir, default is self.pickle_dir

        Returns:
            The loaded object.
        """

        if version == 0:
            try:
                version = self.versions(source_dir)[obj_name]
            except KeyError:
                raise FileNotFoundError(f"No files matching {obj_name}.n.pickle found in {source_dir}")
        file_name = os.path.join(source_dir, f"{obj_name}.{version}.pickle")
        try:
            with open(file_name, 'rb') as f:
                obj = pickle.load(f)
                #print(f"Loaded {obj_name} (version {version})")
                return obj
        except Exception as e:
            logger.warning(
                f"Failed to unpickle {file_name}.\n{str(e)}"
            )

    def __getitem__(self, item):
        return self.objects[item]





def _test_picklepot(tmpdir='/home/jthomas/tmp/pickles/'):
    tmpdir = pathlib.Path(tmpdir)
    i0 = 'xxx'
    i1 = 'aaa'
    i2 = 'fff'
    p = PicklePot(tmpdir, pickle_dir='fart')
    p.dump(i0, 'o1', 'v0')
    p.dump(i1, 'o1', 'v1')
    p.dump(i1, 'o1', 'v1 overwrit', overwrite_latest=True)
    p.dump(i2, 'o2')
    p2 = PicklePot(tmpdir, pickle_dir='fart2')

    # can load specific ver
    assert  i0 == p.load_obj('o1', version=1)
    # auto load objects match
    assert p2['o1'] == i1
    assert p2['o2'] == i2

    print('ledger:', p.ledger())
    print('ledger, latest only', p.ledger(latest_only=True))
    print('ver history:', p.version_history('o1', print_it=True))
    import glob
    for fn in  glob.glob(str(tmpdir/'*.pickle')):
        os.remove(fn)
    os.remove(tmpdir/p._ledger_path)

    os.rmdir(tmpdir)

# things to test
# - pickle/unpickle
# - pull specific version
# - overwrite version
# -

if __name__ == '__main__':
    _test_picklepot()


