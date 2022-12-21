import copy
from collections import defaultdict, Counter

class DuplicateTableDetection:

    counter_fp = 0
    counter_superkey = 0
    duplicates = []
    duplicate_tables = []
    enable_print = False

    sorted_row_cache = defaultdict(lambda: defaultdict(list))

    @staticmethod
    def compareTables(t1, t2, data):
        matched_rows = dict()

        duplicates_local = []

        t1_data = data[1][t1]
        t2_data = data[1][t2]

        # Compare num of columns:
        if len(data[0][t1][0]) != len(data[0][t2][0]):
            return None  # Number of columns is different
        # End compare num of columns

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

                if row_t2 not in DuplicateTableDetection.sorted_row_cache[tableId_smaller]:
                    DuplicateTableDetection.sorted_row_cache[tableId_smaller][row_t2] = sorted(rowvalues_t2)

                for row_t1 in hashjoin_map[super_key_t2]:
                    DuplicateTableDetection.counter_superkey = DuplicateTableDetection.counter_superkey+1
                    rowvalues_t1 = list(data[0][tableId_bigger][row_t1].values())
                    if len(rowvalues_t1) <= 0:
                        continue

                    # generate and store
                    map1 = defaultdict(set)
                    for i in range(len(rowvalues_t1)):
                        map1[rowvalues_t1[i]].add(i)

                    if row_t1 not in DuplicateTableDetection.sorted_row_cache[tableId_bigger]:
                        DuplicateTableDetection.sorted_row_cache[tableId_bigger][row_t1] = sorted(rowvalues_t1)

                    fail = False
                    for i in range(0, len(rowvalues_t1)):
                        if DuplicateTableDetection.sorted_row_cache[tableId_smaller][row_t2][i] != DuplicateTableDetection.sorted_row_cache[tableId_bigger][row_t1][i]:
                            # fail, different values
                            fail = True
                            break
                        else:
                            found_cm = False
                            for y in map1[DuplicateTableDetection.sorted_row_cache[tableId_smaller][row_t2][i]]:
                                if y in column_mapping:
                                    for j in column_mapping[y]:
                                        if j in map2[DuplicateTableDetection.sorted_row_cache[tableId_smaller][row_t2][i]]:
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

                        if DuplicateTableDetection.enable_print:
                            print("Dup row")
                        DuplicateTableDetection.duplicates.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
                        duplicates_local.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
                        matched_rows[row_t2] = row_t1
                    else:
                        if DuplicateTableDetection.enable_print:
                            print("fail - False positive")
                        DuplicateTableDetection.counter_fp = DuplicateTableDetection.counter_fp + 1
                    ## End duplicate

        if len(smaller_table) > 0 and len(smaller_table) == len(matched_rows):
            DuplicateTableDetection.duplicate_tables.append((t1, t2))

    @staticmethod
    def getFp():
        return DuplicateTableDetection.counter_fp

    @staticmethod
    def getSum():
        return DuplicateTableDetection.counter_superkey