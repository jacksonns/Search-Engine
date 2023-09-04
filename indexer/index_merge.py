from threading import Thread, Lock
import os

MAX_THREADS = 5

class IndexMerge:

    def __init__(self, path_to_index, path_to_partial_index):
        self.path_to_index = path_to_index
        self.path_to_partial_index = path_to_partial_index

        self.file_list = []
        self.has_file_to_be_merged = True
        self.merge_level = 0

        self.lock_file_list = Lock()
        self.lock_list_counter = Lock()

        self.number_of_lists = 0
        self.lists_size = 0
    
    def set_file_list(self):
        self.file_list = os.listdir(self.path_to_partial_index)
    
    def get_data(self):
        file_size = os.path.getsize(self.path_to_index)
        return file_size, self.number_of_lists, self.lists_size

    def create_merged_file(self, file1, file2, merge_level):
        f1 = open(os.path.join(self.path_to_partial_index, file1), 'r')
        f2 = open(os.path.join(self.path_to_partial_index, file2), 'r')
        merged_file = 'merged_{}.txt'.format(merge_level)
        mf = open(os.path.join(self.path_to_partial_index, merged_file), 'a')

        line1 = f1.readline()
        line2 = f2.readline()
        while (line1 and line2):
            token1, list1 = line1.strip().split(maxsplit=1)
            token2, list2 = line2.strip().split(maxsplit=1)
            # Merge Inverted Lists
            if token1 == token2:
                mf.write('{} {}\n'.format(token1, list1+list2))
                list_size = len(list1.strip(';').split(';')) + len(list2.strip(';').split(';'))
                line1 = f1.readline()
                line2 = f2.readline()
            # Write token1 and keep token2 to be compared
            elif token1 < token2:
                mf.write('{} {}\n'.format(token1, list1))
                list_size = len(list1.strip(';').split(';'))
                line1 = f1.readline()
            # Write token2 and keep token1 to be compared
            else:
                mf.write('{} {}\n'.format(token2, list2))
                list_size = len(list2.strip(';').split(';'))
                line2 = f2.readline()
            
            if not self.has_file_to_be_merged:
                with self.lock_list_counter:
                    self.number_of_lists += 1
                    self.lists_size += list_size
        
        if line1:
            while(line1):
                token1, list1 = line1.strip().split(maxsplit=1)
                mf.write('{} {}\n'.format(token1, list1))
                list_size = len(list1.strip(';').split(';'))
                if not self.has_file_to_be_merged:
                    with self.lock_list_counter:
                        self.number_of_lists += 1
                        self.lists_size += list_size
                line1 = f1.readline()

        if line2:
            while(line2):
                token2, list2 = line2.strip().split(maxsplit=1)
                mf.write('{} {}\n'.format(token2, list2))
                list_size = len(list2.strip(';').split(';'))
                if not self.has_file_to_be_merged:
                    with self.lock_list_counter:
                        self.number_of_lists += 1
                        self.lists_size += list_size
                line2 = f2.readline()
            
        #DELETE FILE1 AND FILE2
        os.remove(os.path.join(self.path_to_partial_index, file1))
        os.remove(os.path.join(self.path_to_partial_index, file2))
    
    def merge_partial_indexes(self):
        while(True):
            with self.lock_file_list:
                try:
                    file1 = self.file_list.pop()
                    file2 = self.file_list.pop()
                except:
                    return
                self.merge_level +=1
                merge_level = self.merge_level
            self.create_merged_file(file1, file2, merge_level)

    def run_index_merge(self):
        while(self.has_file_to_be_merged):
            self.set_file_list()

            if len(self.file_list) == 1:
                self.has_file_to_be_merged = False
                file = self.file_list.pop()
                merged_file = 'merged_{}.txt'.format(self.merge_level)
                os.replace(os.path.join(self.path_to_partial_index, file), 
                           os.path.join(self.path_to_partial_index,merged_file))
                break

            if len(self.file_list) == 2: 
                self.has_file_to_be_merged = False
                threads = [Thread(target=self.merge_partial_indexes)]
            else:
                threads = [Thread(target=self.merge_partial_indexes) for i in range(MAX_THREADS)]
            
            for t in threads:
                t.start()

            for t in threads:
                t.join()
        
        merged_file = 'merged_{}.txt'.format(self.merge_level)
        os.replace(os.path.join(self.path_to_partial_index, merged_file), self.path_to_index)
        os.rmdir(self.path_to_partial_index)
        