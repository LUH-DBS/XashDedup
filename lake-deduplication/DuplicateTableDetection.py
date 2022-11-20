class DuplicateTableDetection:

    counter_fp = 0
    counter_superkey = 0
    duplicates = []
    duplicate_tables = []
    enable_print = False

    @staticmethod
    def compareTables(t1, t2, data):
        duplicates_local = []

        t1_data = data[1][t1]
        t2_data = data[1][t2]

        # Compare num of columns:
        if len(data[0][t1][0]) != len(data[0][t2][0]):
            return None  # Number of columns is different
        # End compare num of columns

        column_mapping = dict()
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
                map2 = dict()
                for i in range(len(rowvalues_t2)):
                    map2[rowvalues_t2[i]] = i

                rowvalues_t2.sort()

                for row_t1 in hashjoin_map[super_key_t2]:
                    DuplicateTableDetection.counter_superkey = DuplicateTableDetection.counter_superkey+1
                    rowvalues_t1 = list(data[0][tableId_bigger][row_t1].values())
                    if len(rowvalues_t1) <= 0:
                        continue

                    map1 = dict()
                    for i in range(len(rowvalues_t1)):
                        map1[rowvalues_t1[i]] = i

                    rowvalues_t1.sort()

                    # Duplicate detection
                    bigger_row = rowvalues_t1
                    smaller_row = rowvalues_t2

                    fail = False
                    for i in range(0, len(bigger_row)):
                        if i >= len(smaller_row):
                            # fail
                            if DuplicateTableDetection.enable_print:
                                print("Fail i")
                            fail = True
                            break
                        if bigger_row[i] != smaller_row[i]:
                            # fail, different values
                            fail = True
                            break
                        else:
                            if map1[bigger_row[i]] not in column_mapping:
                                column_mapping[map1[bigger_row[i]]] = map2[smaller_row[i]]
                            else:
                                if column_mapping[map1[bigger_row[i]]] == map2[smaller_row[i]]:
                                    continue
                                else:
                                    fail = True
                                    break


                    if not fail:
                        if DuplicateTableDetection.enable_print:
                            print("Dup row")
                        DuplicateTableDetection.duplicates.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
                        duplicates_local.append({"tableid_1": t1, "rowid_1": row_t1, "tableid_2": t2, "rowid_2": row_t2})
                    else:
                        if DuplicateTableDetection.enable_print:
                            print("fail - False positive")
                        DuplicateTableDetection.counter_fp = DuplicateTableDetection.counter_fp + 1
                    ## End duplicate

        num_rows_min = min(len(t1_data), len(t2_data))
        if len(duplicates_local) >= num_rows_min and num_rows_min > 0:
            t1_dup = []
            t2_dup = []
            for value in duplicates_local:
                t1_dup.append(value['rowid_1'])
                t2_dup.append(value['rowid_2'])

            if (len(set(t1_dup)) >= len(t1_data) or len(set(t2_dup)) >= len(t2_data)):
                if DuplicateTableDetection.enable_print:
                    print("found duplicate table: " + str(t1) + " and " + str(t2))
                DuplicateTableDetection.duplicate_tables.append((t1, t2))

    @staticmethod
    def getFp():
        return DuplicateTableDetection.counter_fp

    @staticmethod
    def getSum():
        return DuplicateTableDetection.counter_superkey