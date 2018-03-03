import struct, time, os, zlib, io, shutil, sys, getopt
from nbt import nbt

class McFile: #{{{

    def __init__(self, path, isnew = False): #{{{
        self.path = path

        self.offsets = [(0,0)] * 1024
        self.times = [0] * 1024

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

            self.offsets[i] = (offset, blen)
            self.times[i] = time_stamp
    #}}}

    def print_chunks(self): #{{{
        for i in range(0, 1024):
            local_time = time.localtime(self.times[i])
            dt = time.strftime('%Y-%m-%d %H:%M:%S',local_time)
            if self.times[i] == 0 and self.offsets[i][0] == 0:
                continue

            line = "%4d => %d[%d]:%s\n" % (i, self.offsets[i][0], self.offsets[i][1], dt)
            print(line)

            time_stamp,data = self.get_trunk(i)

            tmp_file = io.BytesIO(data)
            nbtfile = nbt.NBTFile(buffer=tmp_file)
            tmp_str = nbtfile.pretty_tree()
            print(tmp_str)
            tmp_file.close()
            return True
    #}}}

    def add_trunk(self, index, blocks, time_stamp, data): #{{{
        self.offsets[index] = (self.cur_offset, blocks)
        self.times[index] = time_stamp

        self.fh.seek(self.cur_offset * 4096, 0)
        self.fh.write(data)

        self.cur_offset += blocks
        self.fh.truncate(self.cur_offset * 4096)
    #}}}

    def get_trunk_raw(self, index): #{{{
        if self.offsets[index][0] == 0:
            return None

        self.fh.seek(self.offsets[index][0] * 4096)
        info = self.fh.read(4)
        data_len, = struct.unpack('>i',info)

        zip_data = self.fh.read(data_len)
        return (info,zip_data)
    #}}}

    def get_trunk(self, index): #{{{
        if self.offsets[index][0] == 0:
            return (0, None)

        info,zip_data = self.get_trunk_raw(index)
        data = zlib.decompress(zip_data[1:])
        return (self.times[index], data)
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

    def move_file(self, dst): #{{{
        new_file = McFile(dst, True)

        for i in range(0, 1024):
            if self.offsets[i][0] == 0 and self.times[i] == 0:
                continue

            info,data = self.get_trunk_raw(i)
            new_file.add_trunk(i, self.offsets[i][1], self.times[i], info + data)

        new_file.write()
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

    def __init__(self, path): #{{{
        path = os.path.abspath(path)
        self.path = path
    #}}}

    def print_files(self): #{{{
        files = os.listdir(self.path)
        for mcfile in files:
            src_file = os.path.join(self.path, mcfile)
            print((src_file + "\n"))
            x = McFile(src_file)
            if x.print_chunks():
                break
    #}}}

    def move_world(self, dst): #{{{
        dst = os.path.abspath(dst)
        if os.path.exists(dst) and not os.path.isdir(dst):
            print('路径不是目录')
            return

        if os.path.exists(dst):
            shutil.rmtree(dst)


        time.sleep(1)
        os.makedirs(dst)

        files = os.listdir(self.path)
        for mcfile in files:
            src_file = os.path.join(self.path, mcfile)
            dst_file = os.path.join(dst, mcfile)
            x = McFile(src_file)
            x.move_file(dst_file)
    #}}}

    def compare(self, right): #{{{
        files = os.listdir(self.path)
        for mcfile in files:
            src_file = os.path.join(self.path, mcfile)
            dst_file = os.path.join(right.path, mcfile)

            if not os.path.exists(dst_file):
                print('缺少：' + mcfile)

            x = McFile(src_file)
            y = McFile(dst_file)
            if x.compare(y):
                print('相同：' + mcfile)
            else:
                print('不同：' + mcfile)
    #}}}

 #}}}

class App: # {{{

    def __init__(self): # {{{
        self.get_param()
    # }}}

    def get_param(self): # {{{
        try:
            arg_len = len(sys.argv)
            if arg_len < 2:
                raise getopt.GetoptError('')

            cmd = sys.argv[1]

            if cmd == 'help':
                App.print_help()
                sys.exit()

            opts, args = getopt.getopt(sys.argv[2:], "c:", ["config="])

            self.cfg = {}
            for opt, value in opts:
                if opt in ('-c','--config'):
                    self.cfg['config'] = value

            if 'config' in self.cfg:
                print(self.cfg)
            else:
                print('123')
        except getopt.GetoptError as e:
            App.print_help()
    # }}}

    def run(self): # {{{
        pass
        #x = McWorld('region')
        #x.move_world('region2')
        #y = McWorld('region2')
        #x.compare(y)
        #x.print_files()
        #y.print_files()
    # }}}

    @staticmethod
    def print_help(): # {{{
        print('''
usage: run.sh cmd [option]
        ''')
    # }}}

# }}}

if __name__ == '__main__': #{{{
    app = App()
    app.run()
#}}}
