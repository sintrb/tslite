# -*- coding: UTF-8 -*
'''
Created on 2020-02-19
'''

from __future__ import print_function

import datetime, time, os, struct, json

__version__ = '1.0.4'
__version_code__ = 1


def print_bytes(data):
    for d in data:
        print('%02x' % d, end=' ')
    print()


def to_timestamp(tm):
    if isinstance(tm, datetime.datetime):
        tm = time.mktime(tm.timetuple()) + tm.microsecond / 1000000.0
    return tm


type_map = {
    # type: (todb, topy)
    'string': (str, str),
    'int': (str, lambda v: int(float(v or 0))),
    'float': (str, lambda v: float(v or 0)),
    'time': (str, lambda v: float(v or 0)),
}


class Database(object):
    def __init__(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.table_map = {}

    def get_table(self, tabname):
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        if tabname not in self.table_map:
            self.table_map[tabname] = Table(self, tabname)
        return self.table_map[tabname]

    def drop(self):
        for tab in self.table_map.values():
            tab.close()
        if os.path.exists(self.path):
            import shutil
            shutil.rmtree(self.path)
        self.table_map.clear()

    def drop_table(self, tabname):
        tab = self.get_table(tabname)
        tab.remove()
        del self.table_map[tabname]

    def commit(self):
        for k, v in self.table_map.items():
            v.commit()

    def close(self):
        for k, v in self.table_map.items():
            v.commit()
            v.close()
        self.table_map.clear()


class File(object):
    def __init__(self, path):
        self.path = path
        self.file = open(self.path, 'ab+')
        self._last = self.tell()

    def read(self, size=None):
        return self.file.read(size)

    def write(self, data):
        # print(self.path, 'write', len(data), '@', self.tell())
        w = self.file.write(data)
        self._last = self.file.tell()
        return w

    def seek(self, offset):
        self.file.seek(int(offset))

    def tell(self):
        return self.file.tell()

    def close(self):
        self.file.close()

    def flush(self):
        self.file.flush()

    def remove(self):
        self.close()
        os.remove(self.path)

    def get_last(self):
        return self._last

    def set_last(self, last):
        self._last = last
        self.seek(0)
        self.file.truncate(last)
        self.seek(last)

    def to_last(self):
        return self.file.seek(self._last)


class Index(object):
    __slots__ = ('line', 'stamp', 'offset')
    FORMATTER = '<LdL'
    LENGTH = struct.calcsize(FORMATTER)

    def __init__(self, data=None, line=0, stamp=0.0, offset=0):
        if data != None:
            self.slots = struct.unpack(Index.FORMATTER, data)
        else:
            self.slots = line, stamp, offset

    @property
    def slots(self):
        return (self.line, self.stamp, self.offset)

    @slots.setter
    def slots(self, slots):
        (self.line, self.stamp, self.offset) = slots

    def pack(self):
        return struct.pack(Index.FORMATTER, *self.slots)

    def __str__(self):
        return str(self.slots)


class IndexFile(File):
    _changed = True

    def tell_line(self):
        return int(self.tell() / Index.LENGTH)

    def last_line(self):
        return int(self.get_last() / Index.LENGTH)

    def read_index(self, line):
        self._changed = True
        if line != None:
            self.seek(line * Index.LENGTH)
        data = self.read(Index.LENGTH)
        ix = Index(data=data)
        return ix

    def read_offset(self, line=None):
        ix = self.read_index(line)
        return ix.offset

    def read_stamp(self, line=None):
        ix = self.read_index(line)
        return ix.stamp

    def write_index(self, offset, stamp):
        if self._changed:
            self.to_last()
        ix = Index(line=self.tell_line(), stamp=stamp, offset=offset)
        self.write(ix.pack())


KEY_FIELD_LOCK = 'field_lock'
KEY_FIELDS = 'fields'


class Table(File):
    _changed = True
    safe_model = False

    def __init__(self, db, name, safe_model=False):
        self.db = db
        self.name = name
        self.tabpath = os.path.join(self.db.path, name)
        self.datapath = self.tabpath + '-db.csv'
        self.safe_model = safe_model
        File.__init__(self, self.datapath)
        self.index = IndexFile(self.tabpath + '-index.bin')
        self.specpath = self.tabpath + '-spec.json'
        self._load_spec()
        self.spec.setdefault(KEY_FIELDS, [])
        self.fields = self.spec[KEY_FIELDS]
        self.fields_map = {}

        self.files_map = {
            'db.csv': self,
            'index.bin': self.index
        }

        self._pop_file_seek()

    def _save_spec(self):
        with open(self.specpath, 'w') as f:
            json.dump(self.spec, f)

    def _load_spec(self):
        if os.path.exists(self.specpath):
            self.spec = json.load(open(self.specpath))
        else:
            self.spec = {}

    def _pop_file_seek(self):
        files_seek = self.spec.get('files_seek', {})
        for k, v in self.files_map.items():
            if k not in files_seek:
                continue
            v.set_last(files_seek[v])

    def _push_file_seek(self):
        self.spec.setdefault('files_seek', {})
        for k, v in self.files_map.items():
            self.spec[k] = v.get_last()
        if self.safe_model:
            self._save_spec()

    def commit(self):
        self._save_spec()

    def define(self, spec):
        self.spec = {
            k: v for k, v in spec.items() if k not in [KEY_FIELDS]
        }
        self.fields = self.spec[KEY_FIELDS] = []
        for fd in spec.get(KEY_FIELDS, []):
            self._add_field(fd, save=False)
        self._save_spec()

    def _add_field(self, fd, save=True):
        if not self.fields or self.fields[0]['name'] != 'time':
            tfd = {
                'name': 'time',
                'type': 'time',
            }
            self.fields.insert(0, tfd)
            self.fields_map[tfd['name']] = tfd
        self.fields.append(fd)
        self.fields_map[fd['name']] = fd
        if save:
            self._save_spec()

    def _make_line(self, vals):
        line = ','.join(map(str, vals))
        return line

    def _parse_line(self, line):
        vs = line.split(',')
        return vs

    def _write_line(self, data):
        self.write(data)

    def _write_vars(self, vs):
        text = self._make_line(vs)
        text = text.replace('\n', '/n').replace('\r', '/r')
        text = text + '\n'
        data = text.encode('utf8')
        self._write_line(data)

    def _read_line(self):
        return self.file.readline()

    def _value_to_python(self, v, fd):
        mp = type_map.get(fd.get('type', 'string'), (str, str))
        return mp[1](v)

    def _value_to_file(self, v, fd):
        mp = type_map.get(fd.get('type', 'string'), (str, str))
        return mp[0](v)

    def _read_vars(self):
        data = self._read_line()
        text = data.decode('utf8')
        text = text[0:len(text) - 1]
        text = text.replace('/n', '\n').replace('/n', '\n')
        vs = self._parse_line(text)
        vsl = len(vs)
        return {
            f['name']: self._value_to_python(vs[i], f) if i < vsl else f.get('default', '') for i, f in enumerate(self.fields)
        }

    def write_data(self, data):
        return self.write_datas([data])

    def write_datas(self, datas):
        if self._changed:
            self.to_last()
        try:
            self._push_file_seek()
            for data in datas:
                nd = data.copy()
                vs = []
                if 'time' in nd:
                    stamp = to_timestamp(nd.pop('time'))
                else:
                    stamp = time.time()
                nd['time'] = stamp
                offset = self.tell()
                for fd in self.fields:
                    n = fd['name']
                    if n in nd:
                        v = nd.pop(n)
                    else:
                        v = fd.get('default', '')
                    vs.append(self._value_to_file(v, fd))
                if nd and not self.spec.get(KEY_FIELD_LOCK):
                    for k, v in nd.items():
                        fd = {'name': k}
                        self._add_field(fd)
                        vs.append(self._value_to_file(v, fd))
                self.index.write_index(offset, stamp=stamp)
                self._write_vars(vs)
            self.flush()
            self.index.flush()
        except:
            import traceback
            traceback.print_exc()
            self._pop_file_seek()
        return len(datas)

    def read_data(self, line=None):
        self._changed = True
        if line != None:
            of = self.index.read_offset(line)
            self.seek(of)
        return self._read_vars()

    def close(self):
        for k, f in self.files_map.items():
            if f == self:
                File.close(self)
            else:
                f.close()
        self.files_map.clear()

    def remove(self):
        self.close()
        self.index.remove()
        File.remove(self)
        if os.path.exists(self.specpath):
            os.remove(self.specpath)

    def query(self, start=None, end=None, eqs=None):
        _filter = None
        if eqs:
            def ft(d):
                eq = True
                kvs = list(eqs.items())
                for k, v in kvs:
                    if d[k] != v:
                        eq = False
                        break
                return eq

            _filter = ft
        return Cursor(table=self, start=start, end=end, filter_func=_filter)


class No:
    pass


class Cursor(object):
    LEFT = 0
    RIGHT = 1

    def __init__(self, table, start=None, end=None, filter_func=None):
        self.table = table
        self.start = start
        self.end = end
        self._filter = filter_func
        # print(self.start_ix, self.end_ix, 'count')

    _start_ix = No
    _end_ix = No

    @property
    def start_ix(self):
        if self._start_ix == No:
            self._start_ix = self._find_index(self.start, Cursor.LEFT)
        return self._start_ix

    @property
    def end_ix(self):
        if self._end_ix == No:
            self._end_ix = self._find_index(self.end, Cursor.RIGHT)
        return self._end_ix

    def _find_index(self, stamp, direction):
        index = self.table.index
        first = 0
        last = index.last_line() - 1
        left = direction == Cursor.LEFT
        right = direction == Cursor.RIGHT
        if last > first:
            fst = index.read_stamp(first)
            lst = index.read_stamp(last)
            if left and (stamp == None or stamp <= fst):
                return first
            elif right and (stamp == None or stamp >= lst):
                return last

            ix = int((first + last) / 2)
            pst = None
            while ix >= first and ix <= last and first != last:
                pst = index.read_stamp(ix)
                # print(first, last, ix, stamp, pst)
                if pst == stamp:
                    return ix
                elif stamp > pst:
                    first = ix
                else:
                    last = ix
                ix = int((first + last) / 2)
            if pst != None:
                if left and pst > stamp:
                    return first
                elif right and pst < stamp:
                    return last

    def get_iter(self, reverse=False):
        si = self.start_ix
        ei = self.end_ix
        if isinstance(si, int) and isinstance(ei, int):
            if reverse:
                ix = ei
                off = -1
            else:
                ix = si
                off = 1
            while si <= ix <= ei:
                d = self.table.read_data(ix)
                if not self._filter or self._filter(d):
                    yield d
                ix += off

    def __iter__(self):
        return self.get_iter()

    def count(self):
        count = 0
        if self._filter:
            for _ in self:
                count += 1
        else:
            si = self.start_ix
            ei = self.end_ix
            if isinstance(si, int) and isinstance(ei, int):
                count = ei - si + 1
        return count

    def first(self):
        for d in self.get_iter():
            return d

    def last(self):
        for d in self.get_iter(reverse=True):
            return d

    def _get_skip_datas_(self, skip, count, reverse=False):
        ei = self.end_ix
        if not self._filter:
            for r in range(skip, skip + count):
                if r > ei:
                    break
                yield self.table.read_data(r)
        else:
            ix = 0
            ct = 0
            for r in self.get_iter(reverse=reverse):
                if ix >= skip:
                    yield r
                    ct += 1
                    if count != None and ct >= count:
                        break
                ix += 1

    def __getitem__(self, item):
        si = self.start_ix
        ei = self.end_ix
        if type(item) == slice:
            if not self._filter:
                count = self.count()
                if item.start < 0:
                    item = slice(max(0, item.start + count), count)
                if item.stop > count:
                    item.stop = count
                return self._get_skip_datas_(item.start, item.stop)
            else:
                count = None
                if item.start >= 0:
                    skip = item.start
                    if item.stop != None:
                        count = item.stop - item.start
                else:
                    skip = abs(item.start)
                    if item.stop != None:
                        count = abs(item.start - item.stop)
                return self._get_skip_datas_(skip, count)
        else:
            d = None
            if not self._filter:
                if item >= 0 and item <= ei:
                    # 逆向
                    d = self.table.read_data(si + item)
                elif item < 0 and (item + ei + 1) >= si:
                    d = self.table.read_data(item + ei + 1)
            else:
                if item >= 0:
                    skip = item
                else:
                    skip = abs(item) - 1
                for r in self:
                    if skip <= 0:
                        d = r
                        break
                    skip -= 1
            return d
