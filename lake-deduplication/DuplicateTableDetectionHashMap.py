import copy
from collections import defaultdict, Counter

class DuplicateTableDetectionHashMap:

    counter_fp = 0
    counter_superkey = 0
    duplicates = []
    duplicate_tables = []

    @staticmethod
    def compareTables(t1, t2, data):
        matched_rows = dict()

        t1_data = data[1][t1]
        t2_data = data[1][t2]

        if len(data[0][t1][0]) != len(data[0][t2][0]):
            return None  # Number of columns is different

        column_mapping = defaultdict(set)
        hashjoin_map = dict()

        if(len(t1_data) > len(t2_data)):
            bigger_table = t1_data
            tableId_bigger = t1
            smaller_table = t2_data
            tableId_smaller = t2
        else:
            bigger_table = t2_data
            tableId_bigger = t2
            smaller_table = t1_data
            tableId_smaller = t1

        for row_t1 in bigger_table:
            super_key_t1 = bigger_table[row_t1]
            if super_key_t1 not in hashjoin_map:
                hashjoin_map[super_key_t1] = []
            hashjoin_map[super_key_t1].append(row_t1)

        for row_t2 in smaller_table:
            super_key_t2 = smaller_table[row_t2]
            if super_key_t2 not in hashjoin_map:
                break
            else:
                rowvalues_t2 = list(data[0][tableId_smaller][row_t2].values())

                # generate and store
                map2 = defaultdict(set)
                for i in range(len(rowvalues_t2)):
                    map2[rowvalues_t2[i]].add(i)

                count = {}
                for i in rowvalues_t2:
                    if i in count:
                        count[i] += 1
                    else:
                        count[i] = 1


                for row_t1 in hashjoin_map[super_key_t2]:
                    DuplicateTableDetectionHashMap.counter_superkey = DuplicateTableDetectionHashMap.counter_superkey+1
                    rowvalues_t1 = list(data[0][tableId_bigger][row_t1].values())
                    if len(rowvalues_t1) <= 0:
                        continue

                    # generate and store
                    map1 = defaultdict(set)
                    for i in range(len(rowvalues_t1)):
                        map1[rowvalues_t1[i]].add(i)

                    fail = False

                    count_copy = copy.deepcopy(count)
                    for i in rowvalues_t1:
                        # Check if value in hashmap
                        if i not in count_copy or count_copy[i] == 0:
                            fail = True
                            break
                        else:
                            count_copy[i] -= 1
                            found_cm = False
                            for y in map1[i]:
                                if y in column_mapping:
                                    for j in column_mapping[y]:
                                        if j in map2[i]:
                                            found_cm = True
                                            break
                                else:
                                    found_cm = True
                            if not found_cm:
                                fail = True # Column mapping mismatch
                                break

                    if not fail:
                        for i in rowvalues_t1:
                            for y in map1[i]:
                                if y not in column_mapping:
                                    column_mapping[y].update(map2[i])

                        DuplicateTableDetectionHashMap.duplicates.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2}) # Duplicate row
                        matched_rows[row_t2] = row_t1 # Used to check if duplicate table
                    else:
                        DuplicateTableDetectionHashMap.counter_fp = DuplicateTableDetectionHashMap.counter_fp + 1 # False positive
                    ## End duplicate

        if len(smaller_table) > 0 and len(smaller_table) == len(matched_rows):
            DuplicateTableDetectionHashMap.duplicate_tables.append((t1, t2)) # Check if found duplicate rows are duplicate table

    @staticmethod
    def getFp():
        return DuplicateTableDetectionHashMap.counter_fp

    @staticmethod
    def getSum():
        return DuplicateTableDetectionHashMap.counter_superkey