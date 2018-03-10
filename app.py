#coding=utf-8

import struct, time, os, zlib, io, shutil, sys, getopt, gzip, json, copy
from nbt import nbt

class RegionList: # {{{

    @staticmethod
    def match_range(left, right): # {{{
        if len(left) < 2:
            return right
        l = max(left[0], right[0])
        r = min(left[1], right[1])
        if l <= r:
            return [l, r]
        return None
    # }}}

    @staticmethod
    def match_region(region, to_match): # {{{
        x = RegionList.match_range(region['x'], to_match['x'])
        z = RegionList.match_range(region['z'], to_match['z'])
        if x == None or z == None:
            return None
        ret = {}
        ret['type'] = region['type']
        ret['x'] = x
        ret['z'] = z
        return ret
    # }}}

    @staticmethod
    def format_range(data, div = False): # {{{
        ret = []
        if len(data) == 0:
            ret = []
        elif len(data) == 1:
            ret = [data[0], data[0]]
        elif data[0] > data[1]:
            ret = [data[1], data[0]]
        else:
            ret = [data[0], data[1]]
        if div != False:
            ret[0] = ret[0] // div
            ret[1] = ret[1] // div
        return ret
    # }}}

    def add_region(self, region, div = False): # {{{
        tmp = {}
        tmp['type'] = region['type']
        if 'x' in region:
            tmp['x'] = RegionList.format_range(region['x'], div)
        else:
            tmp['x'] = []
        if 'z' in region:
            tmp['z'] = RegionList.format_range(region['z'], div)
        else:
            tmp['z'] = []
        self.area.append(tmp)
    # }}}

    def __init__(self): # {{{
        self.area = []
    # }}}

    def __repr__(self): # {{{
        return 'Region()'
    # }}}

    def __str__(self): # {{{
        return "Region:{\n\tarea:" + self.area.__str__() + "\n}"
    # }}}

    def add(self, region = None, region_list = None, div = False): # {{{
        if region_list != None:
            for region in region_list:
                self.add_region(region, div)
        elif region != None:
            self.add_region(region, div)
    # }}}

    def match(self, to_match): # {{{
        ret_list = []
        for region in self.area:
            item = RegionList.match_region(region, to_match)
            if item != None:
                ret_list.append(item)
        if len(ret_list) <= 0:
            return None
        region_list = RegionList()
        region_list.add(region_list = ret_list)
        return region_list
    # }}}

    def apply(self, base, chunks): # {{{
        for region in self.area:
            x_range = [region['x'][0] - base[0], region['x'][1] - base[0]]
            z_range = [region['z'][0] - base[0], region['z'][1] - base[0]]
            rtype = region['type']
            for x in range(x_range[0], x_range[1] + 1):
                for z in range(z_range[0], z_range[1] + 1):
                    if rtype == 'include':
                        chunks[x + z * 32] = 1
                    else:
                        chunks[x + z * 32] = 0
    # }}}

# }}}

class McChunk: # {{{

    @staticmethod
    def count(blist, bid, cache): # {{{
        if not bid in cache:
            cache[bid] = blist.count(bid)
        return cache[bid]
    # }}}

    def __init(self): # {{{
        self.coord = None
        self.index = -1
        self.time_stamp = -1
        self.data = None
    # }}}

    def set_info(self, coord = None, index = -1, time_stamp = -1): # {{{
        if coord != None:
            self.coord = coord
            self.index = coord[0] % 32 + (coord[1] % 32) * 32
        elif index != -1:
            self.index = index
        if time_stamp != -1:
            self.time_stamp = time_stamp
    # }}}

    def get_info(self): # {{{
        data_len = len(self.data) + 4
        blocks = data_len // 4096
        if data_len % 4096 != 0:
            blocks += 1
        return (self.index, blocks, self.time_stamp)
    # }}}

    def set_data(self, data): # {{{
        self.data = data
    # }}}

    def pack_data(self): # {{{
        data_len = len(self.data)
        return struct.pack('>i', data_len) + self.data
    # }}}

    def set_file(self, file_path): # {{{
        self.file = file_path
    # }}}

    def print_nbt(self, out_file): # {{{
        nbtfile = McRegion.decode_nbt(self.data[1:], 'zlib')
        if nbtfile != None:
            local_time = time.localtime(self.time_stamp)
            dt = time.strftime('%Y-%m-%d %H:%M:%S',local_time)
            print("文件: %s\n区块坐标: (%d, %d)\n索引位置: %d\n修改时间: %s" % (self.file, self.coord[0], self.coord[1], self.index, dt), file=out_file)
            print(nbtfile.pretty_tree(), file=out_file)
        else:
            print('区块数据错误')
    # }}}

    def calc_block(self, args): # {{{
        nbtfile = McRegion.decode_nbt(self.data[1:], 'zlib')
        if nbtfile == None:
            print('区块数据错误')
            sys.exit()

        calc = {}
        if 'calc' in args:
            calc = args['calc']
        y = [0, 255]
        if 'y' in args:
            y = args['y']

        ret = {}
        for name in calc:
            ret[name] = 0

        sections = nbtfile['Level']['Sections']
        for section in sections:
            cy = section['Y'].value
            cy_range = [cy * 16, cy * 16 + 15]
            cy_range = RegionList.match_range(y, cy_range)
            if cy_range == None:
                continue
            cy_range = [cy_range[0] - cy * 16, cy_range[1] + 1 - cy * 16]

            blocks = section['Blocks']
            blocks = section['Blocks'][cy_range[0] * 256:cy_range[1] * 256]
            block_sum = (cy_range[1] - cy_range[0]) * 256
            cache = {}
            for name in calc:
                if 'include' in calc[name]:
                    for bid in calc[name]['include']:
                        ret[name] += McChunk.count(blocks, bid, cache)
                elif 'exclude' in calc[name]:
                    ncount = 0
                    for bid in calc[name]['exclude']:
                        ncount += McChunk.count(blocks, bid, cache)
                    ret[name] += block_sum - ncount
        return ret
    # }}}

# }}}

class McRegion: #{{{

    @staticmethod
    def get_coord(coord_string = None, index = -1): # {{{
        if index >= 0:
            return [index % 32, index // 32]

        if coord_string == None:
            return [0, 0]

        if coord_string.find(',') == -1:
            index = int(coord_string)
            return [index % 32, index // 32]

        coord = [int(i) for i in coord_string.split(',')]
        if(len(coord) != 2):
            return [0, 0]
        return coord
    # }}}

    @staticmethod
    def get_index(coord): # {{{
        return (coord[0] % 32) + (coord[1] % 32) * 32
    # }}}

    @staticmethod
    def decode_nbt(data, compress = None): # {{{
        try:
            if compress == 'zlib':
                data = zlib.decompress(data)
            elif compress == 'gzip':
                data = gzip.decompress(data)
        except Exception as e:
            print(e)
            return None

        try:
            tmp_file = io.BytesIO(data)
            nbtfile = nbt.NBTFile(buffer=tmp_file)
        except Exception as e:
            nbtfile = None
        finally:
            tmp_file.close()

        if nbtfile == None and compress == None:
            try:
                uncompress_data = zlib.decompress(data)
            except Exception as e:
                uncompress_data = None

            if uncompress_data == None:
                try:
                    uncompress_data = gzip.decompress(data)
                except Exception as e:
                    uncompress_data = None

            if uncompress_data == None:
                return None

            try:
                tmp_file = io.BytesIO(uncompress_data)
                nbtfile = nbt.NBTFile(buffer=tmp_file)
            except Exception as e:
                nbtfile = None
            finally:
                tmp_file.close()

        return nbtfile
    # }}}

    def __init__(self, path, isnew = False): #{{{
        self.path = path

        self.offsets = [[0,0]] * 1024
        self.times = [0] * 1024

        filename = os.path.basename(path)
        tmp = filename.split('.');
        if len(tmp) != 4 or tmp[3] != 'mca':
            raise Exception('')
        self.base = [int(tmp[1]) * 32, int(tmp[2]) * 32]

        if not isnew:
            self.fh = open(self.path, 'rb+')
            self.load()
        else:
            self.fh = open(self.path, 'wb+')
            self.cur_offset = 2
            self.fh.truncate(4096 * 2)

    #}}}

    def load(self): #{{{
        offsets_data = self.fh.read(4096)
        times_data = self.fh.read(4096)

        self.cur_offset = 2

        for i in range(0, 1024):
            b1,b2,b3,blen = struct.unpack('4B',offsets_data[i*4:i*4+4])
            offset = (b1 << 16) | (b2 << 8) | b3
            time_stamp, = struct.unpack('>i',times_data[i*4:i*4+4])

            if self.cur_offset < offset + blen:
                self.cur_offset = offset + blen

            self.offsets[i] = [offset, blen]
            self.times[i] = time_stamp
    #}}}

    def show_chunks(self, out_file): #{{{
        for i in range(0, 1024):
            if self.offsets[i][0] == 0:
                continue
            coord = McRegion.get_coord(index = i)
            local_time = time.localtime(self.times[i])
            dt = time.strftime('%Y-%m-%d %H:%M:%S',local_time)

            print('%4d (%2d,%2d) => %s' % (i, coord[0], coord[1], dt), file=out_file)
        return
    #}}}

    def add_chunk(self, chunk): #{{{
        index, blocks, time_stamp = chunk.get_info()

        self.offsets[index] = (self.cur_offset, blocks)
        self.times[index] = time_stamp

        self.fh.seek(self.cur_offset * 4096, 0)
        self.fh.write(chunk.pack_data())

        self.cur_offset += blocks
        self.fh.truncate(self.cur_offset * 4096)
    #}}}

    def get_chunk(self, index = -1, coord = None): #{{{
        if index < 0:
            if coord == None:
                return None
            index = McRegion.get_index(coord)

        if self.offsets[index][0] == 0:
            return None

        chunk = McChunk()
        chunk.set_info(coord = coord, index = index, time_stamp = self.times[index])
        chunk.set_file(self.path)

        self.fh.seek(self.offsets[index][0] * 4096)
        info = self.fh.read(4)
        data_len, = struct.unpack('>i',info)
        zip_data = self.fh.read(data_len)

        chunk.set_data(zip_data)
        return chunk
    #}}}

    def write(self): #{{{
        of_f = io.BytesIO()
        ti_f = io.BytesIO()

        for i in range(0, 1024):
            of_f.write(struct.pack('4B', self.offsets[i][0] >> 16, (self.offsets[i][0] >> 8) & 0xFF, self.offsets[i][0] & 0xFF, self.offsets[i][1]))
            ti_f.write(struct.pack('>i', self.times[i]))

        self.fh.seek(0,0)
        self.fh.write(of_f.getvalue())
        self.fh.write(ti_f.getvalue())

    #}}}

    def move_file(self, index, args, ret): #{{{
        new_file = args['dst_file']
        chunk = self.get_chunk(index = index)
        new_file.add_chunk(chunk)
    #}}}

    def calc_block(self, index, args, ret): #{{{
        chunk = self.get_chunk(index = index)
        count = chunk.calc_block(args)
        if ret == None:
            return count
        for item in ret:
            if item in count:
                count[item] += ret[item]
            else:
                count[item] = ret[item]
        return count
    #}}}

    def walk(self, region_list, call, args): #{{{
        ret = None

        chunks = [0] * 1024
        region_list.apply(self.base, chunks)

        for i in range(0, 1024):
            if self.offsets[i][0] == 0 and self.times[i] == 0:
                continue
            if chunks[i] == 0:
                continue
            ret = call(i, args, ret)
        return ret
    #}}}

    def compare(self, right):#{{{
        for i in range(0, 1024):
            ldata = self.get_trunk(i)
            rdata = right.get_trunk(i)
            if ldata[0] != rdata[0] or ldata[1] != rdata[1]:
                print('chunk: %d [%d %d]' % (i, ldata[0],rdata[0]))
                return False
        return True
    #}}}

#}}}

class McWorld: #{{{

    @staticmethod
    def format_coord(coord_string): # {{{
        if isinstance(coord_string, str):
            if coord_string.find(',') == -1:
                return [0, 0]
            coord = [int(i) for i in coord_string.split(',')]
            if(len(coord) != 2):
                return [0, 0]
            return coord
        return coord_string
    # }}}

    @staticmethod
    def get_chunk_coord(block = None, chunk = None): # {{{
        if chunk != None:
            return McWorld.format_coord(chunk)
        if block != None:
            coord = McWorld.format_coord(block)
            return [coord[0] // 16, coord[1] // 16]

    # }}}

    @staticmethod
    def get_file_region(coord): # {{{
        file_region = {}
        file_region['x'] = [coord[0] * 32, coord[0] * 32 + 31]
        file_region['z'] = [coord[1] * 32, coord[1] * 32 + 31]
        return file_region
    # }}}

    def __init__(self, path): #{{{
        path = os.path.abspath(path)
        self.path = path
    #}}}

    def get_chunk(self, coord): #{{{
        file_name = 'r.%d.%d.mca' % (coord[0] // 16, coord[1] // 16)
        file_path = os.path.join(self.path, file_name)
        if not os.path.exists(file_path):
            return None
        x = McRegion(file_path)
        return x.get_chunk(coord = coord)
    #}}}

    def move_data(self, region_list, file_name, args, ret): #{{{
        print(file_name)
        src_path = os.path.join(self.path, file_name)
        dst = args['dst']
        dst_path = os.path.join(dst, file_name)

        x = McRegion(src_path)
        new_file = McRegion(dst_path, True)

        x.walk(region_list, x.move_file, {'dst_file': new_file})
        new_file.write()
    #}}}

    def calc_block(self, region_list, file_name, args, ret): #{{{
        src_path = os.path.join(self.path, file_name)

        x = McRegion(src_path)
        count = x.walk(region_list, x.calc_block, args)
        if ret == None:
            return count
        for item in ret:
            if item in count:
                count[item] += ret[item]
            else:
                count[item] = ret[item]
        return count
    #}}}

    def walk(self, region_list, call, args): #{{{
        ret = None
        files = os.listdir(self.path)
        for file_name in files:
            tmp = file_name.split('.');
            if len(tmp) != 4 or tmp[3] != 'mca':
                continue
            coord = [int(i) for i in tmp[1:3]]
            file_region = McWorld.get_file_region(coord)

            file_region_list = region_list.match(file_region)
            if file_region_list == None:
                continue
            ret = call(file_region_list, file_name, args, ret)
        return ret
    #}}}

    def compare(self, right): #{{{
        files = os.listdir(self.path)
        for McRegion in files:
            src_file = os.path.join(self.path, McRegion)
            dst_file = os.path.join(right.path, McRegion)

            if not os.path.exists(dst_file):
                print('缺少：' + McRegion)

            x = McRegion(src_file)
            y = McRegion(dst_file)
            if x.compare(y):
                print('相同：' + McRegion)
            else:
                print('不同：' + McRegion)
    #}}}

 #}}}

class App: # {{{

    @staticmethod
    def print_help(): # {{{
        print('''
用法: run.sh 指令 [参数]
        ''')
    # }}}

    def load_cfg(self): # {{{
        if not 'config' in self.cfg:
            return None
        cfg_path = os.path.abspath(self.cfg['config'])
        try:
            with open(cfg_path, 'r', encoding = 'utf-8') as f:
                config = f.read()
                config = json.loads(config)
        except Exception as e:
            config = None

        basedir = os.path.dirname(cfg_path)

        if 'src' in config:
            config['src'] = os.path.join(basedir, config['src'])
        if 'dst' in config:
            config['dst'] = os.path.join(basedir, config['dst'])

        return config
    # }}}

    def __init__(self): # {{{
        self.cmd = ''
        self.get_param()
    # }}}

    def get_param(self): # {{{
        try:
            arg_len = len(sys.argv)
            if arg_len < 2:
                raise getopt.GetoptError('')

            self.cmd = sys.argv[1]

            if self.cmd == 'help':
                App.print_help()
                sys.exit()

            opts, args = getopt.getopt(sys.argv[2:], 'C:F:D:O:c:d:b:', ['config=','file=','dir=','output=','chunk','compress','block'])

            self.cfg = {}
            for opt, value in opts:
                if opt in ('-C', '--config'):
                    self.cfg['config'] = value
                elif opt in ('-F', '--file'):
                    self.cfg['file'] = value
                elif opt in ('-D', '--dir'):
                    self.cfg['dir'] = value
                elif opt in ('-O', '--output'):
                    self.cfg['output'] = value
                elif opt in ('-c', '--chunk'):
                    self.cfg['chunk'] = value
                elif opt in ('-b', '--block'):
                    self.cfg['block'] = value
                elif opt in ('-d', '--compress'):
                    self.cfg['compress'] = value

        except getopt.GetoptError as e:
            App.print_help()
            sys.exit()
    # }}}

    def export_file_nbt(self, path, out_file): # {{{
        if 'chunk' in self.cfg:
            x = McRegion(path)
            coord = McRegion.get_coord(self.cfg['chunk'])
            chunk = x.get_chunk(coord = coord)
            chunk.print_nbt(out_file)
        else:
            in_file = open(path, 'rb')
            data = in_file.read()
            in_file.close()

            compress = None
            if 'compress' in self.cfg:
                compress = self.cfg['compress']

            nbtfile = McRegion.decode_nbt(data, compress)
            if nbtfile != None:
                print(nbtfile.pretty_tree(), file=out_file)
            else:
                print('文件格式错误')
    # }}}

    def export_dir_nbt(self, path, out_file): # {{{
        if 'block' in self.cfg:
            coord = McWorld.get_chunk_coord(block = self.cfg['block'])
        elif 'chunk' in self.cfg:
            coord = McWorld.get_chunk_coord(chunk = self.cfg['chunk'])
        else:
            print('参数错误')
            return

        x = McWorld(path)
        chunk = x.get_chunk(coord = coord)
        if chunk != None:
            chunk.print_nbt(out_file)
        else:
            print('区块不存在')
    # }}}

    def do_nbt(self): # {{{
        out_file = None
        if 'output' in self.cfg:
            out_file = open(self.cfg['output'],'w+')

        if 'file' in self.cfg:
            return self.export_file_nbt(self.cfg['file'], out_file)
        elif 'dir' in self.cfg:
            return self.export_dir_nbt(self.cfg['dir'], out_file)
        else:
            print('参数错误')

        if out_file != None:
            out_file.close()
    # }}}

    def do_list_chunks(self): # {{{
        if not 'file' in self.cfg:
            print('参数错误')
            return

        out_file = None
        if 'output' in self.cfg:
            out_file = open(self.cfg['output'],'w+')

        x = McRegion(self.cfg['file'])
        x.show_chunks(out_file)

        if out_file != None:
            out_file.close()
    # }}}

    def do_clear(self): # {{{
        config = self.load_cfg()
        if config == None:
            print('配置文件错误')
            return

        if not 'src' in config or not 'dst' in config:
            print('配置文件错误')
            return

        if not 'area' in config or len(config['area']) <= 0:
            print('配置文件错误')
            return

        region_list = RegionList()
        region_list.add(region_list = config['area'], div = 16)

        dst = config['dst']
        if os.path.exists(dst) and not os.path.isdir(dst):
            print('路径不是目录')
            return
        if not os.path.exists(dst):
            os.makedirs(dst)

        world = McWorld(config['src'])
        world.walk(region_list, world.move_data, {'dst': config['dst']})
    # }}}

    def do_calc_block(self): # {{{
        config = self.load_cfg()
        if config == None:
            print('配置文件错误')
            return

        if not 'src' in config:
            print('配置文件错误')
            return

        if not 'area' in config or len(config['area']) <= 0:
            print('配置文件错误')
            returl

        region_list = RegionList()
        region_list.add(region_list = config['area'], div = 16)

        args = {}
        if not 'calc' in config:
            print('配置文件错误')
            return

        if 'y' in config:
            y = RegionList.format_range(config['y'])
            if y[0] >= 0 or y[1] <= 255:
                args['y'] = y

        args['calc'] = {}
        for item in config['calc']:
            if 'include' in item:
                args['calc'][item['name']] = {}
                args['calc'][item['name']]['include'] = set(item['include'])
            elif 'exclude' in item:
                args['calc'][item['name']] = {}
                args['calc'][item['name']]['exclude'] = set(item['exclude'])

        if len(args['calc']) <= 0:
            print('配置文件错误')
            return


        world = McWorld(config['src'])
        count = world.walk(region_list, world.calc_block, args)

        len1 = 0
        len2 = 0
        for item in config['calc']:
            if item['name'] in count:
                name = item['name']
                count[name] = format(count[name], ',')
                if len(name) > len1:
                    len1 = len(name)
                if len(count[name]) > len2:
                    len2 = len(count[name])

        format_str = '%%-%ds\t%%%ds' % (len1, len2)
        for item in config['calc']:
            if item['name'] in count:
                print(format_str % (item['name'], count[item['name']]))
    # }}}

    def run(self): # {{{
        if self.cmd == 'nbt':
            return self.do_nbt()
        elif self.cmd == 'list':
            return self.do_list_chunks()
        elif self.cmd == 'clear':
            return self.do_clear()
        elif self.cmd == 'calc':
            return self.do_calc_block()
        else:
            print('指令不存在')
            return
    # }}}

# }}}

if __name__ == '__main__': #{{{
    app = App()
    app.run()
#}}}
