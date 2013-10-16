import csv
from time import clock
import datetime
import pickle
from collections import namedtuple
from collections import OrderedDict
import sys

problem_name = 'vistaprint'
delim = ';'

missing_val = 'blank'
missing_int = 9999999

# Sequence to run everything.
# Don't actually run this, since some unix sorts need to be done manually in terminal shell
def run_everything():
    
    # put the downloaded file in standard CSV format, which is semicolon delmited.
    # Also, strip short lines. This function doesn't always need to be used.
    standardize_file() # > name_raw.csv

    # read the column names from the raw file
    read_raw_column_names() # This is called by each function that needs it.

    # read the fields to use
    read_field_selections() # This is called by each function that needs it.
    
    # reduce the file to just the fields we are interested in
    reduce_file() # > name_reduced.csv

    # create conversion tables for the depvar and the attributes
    create_int_conversion_tables() 

    # use the conversation tables to convert depvar and the attributes to ints
    convert_to_ints() # > name_int.csv
    
    # sort based on time, most recent first
    # To get the key range, run the following:
    sort_keys();
    #sort --field-separator=';' --key=11,11 --reverse --output=ds3_int_sorted.csv ds3_int.csv

    # split the dataset into tst and trn (e.g. 1/5, 4/5)
    split_into_tst_trn() # <<< NOT TESTED RECENTLY

    # If you didn't run the above, rename name_int.csv to name_trn_unsorted.csv

    # sort both the trn and tst files by depvar and attributes.
    # To get the key range, run the following:
    calc_sort_columns();
    #sort --field-separator=';' --key=1,12  --output=ds3_tst_sorted.csv ds3_tst_unsorted.csv
    #sort --field-separator=';' --key=1,12  --output=ds3_trn_sorted.csv ds3_trn_unsorted.csv

    compress_with_copies('trn')


# Put the file into a standard csv format
def standardize_file(infile_name = problem_name + '_download.csv', outfile_name = problem_name + '_raw.csv'):
    dataReader = csv.reader(open(infile_name, 'r'), delimiter=delim, quoting=csv.QUOTE_NONE)
    print ('Reading file {}'.format(infile_name))
    f = open(outfile_name, 'w')
    writer = csv.writer(f, delimiter=';')
    print('Writing file', outfile_name)

    line_cnt = 0
    write_cnt = 0
    for row in dataReader:
        line_cnt += 1
        if line_cnt == 1:
            target_cnt = len(row)
            print("Number of columns found:", target_cnt)
            #sys.exit()
        if len(row) != target_cnt:
            #print("Line:", line_cnt)
            #print("Target: ", target_cnt, "  Count:", len(row))
            #print("Skipped:", row)
            continue;
        line = [s.strip() for s in row]
        writer.writerow(line)
        write_cnt += 1
        if (line_cnt % 100000 == 0):
            print(line_cnt)
        #if line_cnt > 10: break

    print((line_cnt, write_cnt, line_cnt-write_cnt), "lines (read, written, diff)")

# Ignore this. Used for testing.
def scan_julian2(infile_name = problem_name + '-adjusted.csv', outfile_name = problem_name + '_julian.csv'):
    path = "/Users/Gil/GilFiles/Tapad Data/vistaprint/"
    
    dataReader = csv.reader(open(path+infile_name, 'r'), delimiter=delim, quoting=csv.QUOTE_NONE)
    print ('Reading file {}'.format(infile_name))
    f = open(outfile_name, 'w')
    writer = csv.writer(f, delimiter=';')
    print('Writing file', outfile_name)

    line_cnt = 0
    write_cnt = 0
    d = dict()
    for row in dataReader:
        line_cnt += 1
        if line_cnt == 1:
            target_cnt = len(row)
            print("Number of columns found:", target_cnt)
            continue # skip header
        if len(row) != target_cnt:
            print("Skipped:", row)
            continue
        dt = datetime.datetime.fromtimestamp(float(row[1]))
        #print(dt)
        julian = dt.timetuple().tm_yday
        if julian not in d:
            d[julian] = [0,0,0]
        l = d[julian]
        if row[0] == 'impression':
            l[0] += 1
        if row[0] == 'Vistaprint_Conversion_Pixel':
            l[1] += 1
        if row[0] == 'click':
            l[2] += 1
            

    return d
    writer.writerow(line)
    write_cnt += 1
    if (line_cnt % 100000 == 0):
        print(line_cnt)
    #if line_cnt > 10: break

    print((line_cnt, write_cnt, line_cnt-write_cnt), "lines (read, written, diff)")

# Read csv files that define the fields
def read_field_selections(infile_name = 'fieldselection.csv', trace=True):

    fields = dict()
    fields['depvar_name'] = 'unknown'
    fields['depvar_type'] = 'unknown'
    fields['attrs'] = OrderedDict()
    fields['data'] = OrderedDict()
    
    dataReader = csv.reader(open(infile_name, 'r'), delimiter=',', quoting=csv.QUOTE_NONE)
    print ('Reading file {}'.format(infile_name))

    line_cnt = 0
    write_cnt = 0
    for row in dataReader:
        if len(row) == 0: continue
        if row[0][0] == '#': continue
        if row[0] == 'dep_var':
            fields['depvar_name'] = row[1]
            fields['depvar_type'] = row[2]
        elif row[0] == 'attr':
            fields['attrs'][row[1]] = row[2]
        elif row[0] == 'data':
            fields['data'][row[1]] = row[2]
        else:
            print('Unrecognized:', row)

    if trace:
        print('Depvar:')
        print('\t', fields['depvar_name'], fields['depvar_type'])
        print('Attributes')
        for k,v in fields['attrs'].items():
            print('\t',k,v)
        print('Data')
        for k,v in fields['data'].items():
            print('\t',k,v)
    return fields

# Read the header line from a csv file and return a namedtuple template
def read_column_names(suffix, trace=True):
    file_name = problem_name + suffix + '.csv'
    dataReader = csv.reader(open(file_name, 'r'), delimiter=delim, quoting=csv.QUOTE_NONE)
    print ('Reading file {}'.format(file_name))
    global raw_cols
    for row in dataReader:
        raw_cols = row
        break
    raw_cols = [s.strip() for s in raw_cols]
    if trace:
        print('Column names:', raw_cols)
    return namedtuple('Fields', raw_cols, verbose=False)

# Print the column ranges to sort the csv files with the unix sort command
def sort_keys(time = 'created_at'):
    fields = read_field_selections(trace=False) # read in the fields to use
    FieldsNT = read_column_names('_reduced', trace=False)

    # create a list of attributes to sort on
    attr_list = [fields['depvar_name']] + [k for k in fields['attrs'].keys()]
    
    num_fields = len(attr_list)
    print("Key range for duplicate sort:", (1, num_fields))
    data_list = [k for k in fields['data'].keys()]
    if time in data_list:
        pos = data_list.index(time) + 1
        print("Time stamp location for unix sorting:", num_fields + pos)
    else:
        print(time, "field not found")
        
            
# reduce the file to just those fields we are interested in
def reduce_file(infile_name = problem_name + '_raw.csv', outfile_name = problem_name + '_reduced.csv'):
    fields = read_field_selections() # read in the fields to use
    FieldsNT = read_column_names('_raw', trace=False)
    
    dataReader = csv.reader(open(infile_name, 'r'), delimiter=delim, quoting=csv.QUOTE_NONE)
    print ('Reading file {}'.format(infile_name))
    f = open(outfile_name, 'w')
    writer = csv.writer(f, delimiter=';')
    print('Writing file', outfile_name)

    line_cnt = 0
    write_cnt = 0
    for row in dataReader:
        line_cnt += 1
        
        if line_cnt == 1:
            target_cnt = len(row)
            print("Number of input columns:", target_cnt)
        
        if len(row) != target_cnt:
            #print("Line:", line_cnt)
            #print("Target: ", target_cnt, "  Count:", len(row))
            sys.exit()
            
        line = FieldsNT._make(row)

        # filter any rows here
        if line.action_id == 'click': continue
        
        new_line = [];

        # depvar
        new_line.append(getattr(line, fields['depvar_name']))

        # attributes
        for k in fields['attrs'].keys():
            new_line.append(getattr(line, k))

        # other data items
        for k in fields['data'].keys():
            new_line.append(getattr(line, k))

        #print(new_line)
        writer.writerow(new_line)
        write_cnt += 1
        
        if (line_cnt % 100000 == 0):
            print(line_cnt)
        #if line_cnt > 10: break

    print((line_cnt, write_cnt, line_cnt-write_cnt), "lines (read, written, diff)")

def try_convert2num(s):
    try:
        i = float(s)
        return i
    except ValueError:
        return s
    
# create attr int conversion tables
def create_int_conversion_tables(file_name = problem_name + '_reduced.csv'):
    fields = read_field_selections(trace=False) # read in the fields to use
    FieldsNT = read_column_names('_reduced', trace=False)

    # create a list of attributes to sort on
    attr_list = [fields['depvar_name']] + [k for k in fields['attrs'].keys()]
    print('fields:', attr_list)
    
    cnts = dict()
    for attr in attr_list:
        cnts[attr] = dict()

    impression_cnt = 0
    click_cnt = 0
    
    # scan the datafile and collect stats on each attribute
    print("Reading data file", file_name, end="");
    with open(file_name, 'r') as f:
        dataReader = csv.reader(f, delimiter=delim, quoting=csv.QUOTE_NONE)
        i = 0
        for row in dataReader:
            i += 1
            if (i % 1000000 == 0): print('.{}'.format(i // 1000000), end = "")
            #if i == 100000: break
            
            if i == 1: continue #skip header line
            
            r = FieldsNT._make(row)
            
            if r.action_id == 'impression':
                impression_cnt += 1
            else:
                click_cnt += 1
                
            for attr in attr_list:
                if not getattr(r, attr) in cnts[attr]:
                    cnts[attr][getattr(r, attr)] = [0,0]
                if r.action_id == 'impression':
                    cnts[attr][getattr(r, attr)][0] += 1
                else:
                    cnts[attr][getattr(r, attr)][1] += 1
    print()
    
    for attr in attr_list:
        attr_cnt_list = []
        attr_cnts = cnts[attr]
        for k,v in attr_cnts.items():
            attr_cnt_list.append([k, v[0], v[1], "{:.6f}".format(v[1]/(v[0]+v[1]))])
        #attr_cnt_list.sort(key=lambda item: int(item[0]), reverse=False) # sort on attr value
        if attr in ['day_of_week', 'hour_of_day']:
            attr_cnt_list.sort(key=lambda item: int(item[0]), reverse=False) # sort on attr value
        else:
            attr_cnt_list.sort(key=lambda item: item[0], reverse=False) # sort on attr value           
        if attr in ['action_id']:
             attr_cnt_list.sort(key=lambda item: item[0], reverse=True) # sort on attr value           
           
        # add index number to beginning of list        
        for i in range(len(attr_cnt_list)):
            attr_cnt_list[i] = [i] + attr_cnt_list[i]
                                          
        # write the file
        fname = attr + "_int.csv"
        print("Writing file", fname)
        f = open(fname, 'w')
        writer = csv.writer(f, delimiter=',')
        for row in attr_cnt_list:
            #print(row)
            writer.writerow(row)
        f.close()

        # Write impression/click counts
        f = open(problem_name + "_impression_click_counts.csv", 'w')
        writer = csv.writer(f, delimiter=',')
        writer.writerow([impression_cnt, click_cnt])
        f.close()
        
    print('Impressions:', impression_cnt, 'Clicks:', click_cnt);
                                          
# convert data to ints
def convert_to_ints(infile = problem_name + '_reduced.csv', outfile = problem_name + '_int.csv'):

    fields = read_field_selections(trace=False) # read in the fields to use
    FieldsNT = read_column_names('_reduced', trace=False)

    # create a list of attributes to sort on
    attr_list = [fields['depvar_name']] + [k for k in fields['attrs'].keys()]
    print('fields:', attr_list)
    
    convert = OrderedDict()
    for attr in attr_list:
        convert[attr] = OrderedDict()
        
    # load conversion tables
    for attr in attr_list:
        name = str(attr) + '_int.csv'
        print("Reading",name)
        with open(name, 'r') as f:
            dataReader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
            for row in dataReader:
                #print(row)
                convert[attr][row[1]] = int(row[0])
    #print(convert)
    
    output = open(outfile, 'w')
    writer = csv.writer(output, delimiter=',')
    

    # read, convert, and write
    with open(infile, 'r') as f:
        dataReader = csv.reader(f, delimiter=delim, quoting=csv.QUOTE_NONE)
        i = 0
        print("Reading file", infile, end="")
        for row in dataReader:
            i += 1
            if (i % 1000000 == 0): print('.{}'.format(i // 1000000), end = "")
            if i == 1:
                #writer.writerow(row) # write the header row
                print("Skipping header row:", row)
                continue
            #if i > 10: break

            #print("Row:", row)
            r = FieldsNT._make(row)

            # convert the depvar and the attributes
            new_line = []
            for attr in attr_list:
                new_line.append(convert[attr][str(getattr(r, attr))])

            # copy the data fields
            for k,v in fields['data'].items():
                new_line.append(getattr(r,k))
                
            #print("New:", new_line)
            writer.writerow(new_line)
        print()
        print("Wrote file", outfile)
    
    output.close()

# Compress duplicates by using copies field. Assumes data file is sorted.
# If there is a time stamp field, it is set to 0.
def compress_with_copies(file_type, time_stamp_name = "created_at"):
    fields = read_field_selections(trace=False) # read in the fields to use
    FieldsNT = read_column_names('_reduced', trace=False)

    # Determine if a time stamp field is present
    all_fields = FieldsNT._fields
    time_stamp_present = False
    time_stamp_index = -1
    if time_stamp_name in all_fields:
        time_stamp_present = True
        time_stamp_index = all_fields.index(time_stamp_name)
        print(time_stamp_name, "index:", time_stamp_index)
        
    #print(all_fields)

    # create a list of attributes to dup check on
    attr_list = [fields['depvar_name']] + [k for k in fields['attrs'].keys()]

    infile_name = problem_name + '_' + file_type + '_sorted.csv'
    print('Compressing file:', infile_name);
    
    outfile_name = problem_name + '_' + file_type + '.csv'
    print("Creating file", outfile_name)
    outfile = open(outfile_name, 'w')
    out_writer = csv.writer(outfile, delimiter=',')

    num_attrs = len(attr_list) # all attributes to compare are at the front
    
    first = True;
    copies = 0
    lines_read = 0
    lines_written = 0
    impress_read = 0
    impress_written = 0
    clicks_read = 0
    clicks_written = 0

    depvar_read = dict() # number of depvar values read
    depvar_written = dict() # number of depvar values written
    max_copies = -1 # max number of copies seen
    
    print('Reading file', infile_name, end='')
    with open(infile_name, 'r') as f:
        dataReader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in dataReader:
            lines_read += 1
            #if (lines_read > 100000): break
            
            # (There shouldn't be a header line at this point)

            # Count the number of depvar values read
            if row[0] in depvar_read:
                depvar_read[row[0]] += 1
            else:
                depvar_read[row[0]] = 1
                
            if (lines_read % 1000000 == 0): print('.{}'.format(lines_read // 1000000), end = "")

            if first:
                first = False
                last_row = row
                copies = 1
                continue
            if row[0:num_attrs+1] == last_row[0:num_attrs+1]: #all attrs, plus the action
                copies += 1
            else:
                # write the last row
                new_row = last_row + [copies] # add copies
                out_writer.writerow(new_row)
                lines_written += 1
                if new_row[0] in depvar_written:
                    depvar_written[new_row[0]] += 1
                else:
                    depvar_written[new_row[0]] = 1

                last_row = row
                copies = 1

    # write the last line and close the file               
    new_row = last_row + [copies] # include action_id, add copies, drop time stamp
    out_writer.writerow(new_row)
    lines_written += 1
    if new_row[0] in depvar_written:
        depvar_written[new_row[0]] += 1
    else:
        depvar_written[new_row[0]] = 0

    outfile.close()
    print()
    print("Rows read: {:,}  written: {:,}  diff: {:,}  ratio: {:,.3f}".format(lines_read, lines_written,
                                                                                     lines_read - lines_written,
                                                                                     lines_read / lines_written))
    for k,v in depvar_read.items():
        print(k,v)
        #print("Depvar {:,}: {:,} lines read".format(k, v))
    for k,v in depvar_written.items():
        print(k,v)
        #print("Depvar {:,}: {:,} lines written".format(k, v))

    

def str2int(s):
    if s == str(missing_val):
        print('Int in s2int = ', missing_val)
        assert False
    else:
        if s == '': return missing_val
    return int(s)


# convert to string, replace a null string with a missing value string
def to_str(val):
    v = str(val)
    if v == '':
        v = missing_val;
    return v

# convert to int, replacing a non-int with missing_int value
def to_int(val):
    v = str(val)
    if v.isdigit():
        return int(val)
    else:
        return missing_int

# convert Tapad string to unix time
def convert_to_timestamp(s):
    if s.find('.') == -1:
        d = datetime.datetime.strptime(s+"EDT","%Y-%m-%d %H:%M:%S%Z")
    else:
        d = datetime.datetime.strptime(s+"EDT","%Y-%m-%d %H:%M:%S.%f%Z")
    time_stamp = int(d.strftime("%s"))
    return time_stamp
      

# Split dataset into test and training/val. Test is the first part of the dataset, 
# which have been sorted by decreasing date/time
# **** This has not been used/tested recently ****
def split_into_tst_trn(infile = problem_name + "_int_sorted.csv"):
    # Read number of data records
    impressions = 0
    clicks = 0
    with open('DS3_impression_click_counts.csv', 'r') as f:
        dataReader = csv.reader(f, delimiter=delim, quoting=csv.QUOTE_NONE)
        print("Scanning file", infile, end="")
        i = 0
        for row in dataReader:
            i += 1
            if (i % 1000000 == 0): print('.{}'.format(i // 1000000), end = "")
            impressions = int(row[0])
            clicks = int(row[1])
        print()
        print("{:,} impressions, {:,} clicks, {:.6} ctr".format(
            impressions, clicks, clicks/(clicks+impressions)))

    skip_list = []
    tst_size = int((impressions + clicks) / 5)  # Magic number

    tst_cnt = 0

    test = open(problem_name + '_tst_unsorted.csv', 'w')
    test_writer = csv.writer(test, delimiter=',')
    
    train = open(problem_name + '_trn_unsorted.csv', 'w')
    train_writer = csv.writer(train, delimiter=',')

    trn_cnt = 0
    trn_impress = 0
    trn_clicks = 0
    tst_cnt = 0
    tst_impress = 0
    tst_clicks = 0
    with open(infile, 'r') as f:
        print("Partitioning file", infile, end="")

        dataReader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
        i = 0
        for row in dataReader:
            i += 1
            if (i % 1000000 == 0): print('.{}'.format(i // 1000000), end = "")
            
            r = D1(*row)
            
            if i > tst_size:
                train_writer.writerow(row)
                trn_cnt += 1
                if (r.action_id == '0'):
                    trn_impress += 1
                else:
                    trn_clicks += 1
            else:
                test_writer.writerow(row)
                tst_cnt += 1
                if (r.action_id == '0'):
                    tst_impress += 1
                else:
                    tst_clicks += 1
        print()
                
    test.close()   
    train.close()
    print('Test size {:,}, impressions: {:,}, clicks: {:,}. ctr: {:.6f}'.format(tst_cnt, tst_impress, tst_clicks, tst_clicks/tst_cnt))
    print('Train size {:,}, impressions: {:,}, clicks: {:,}. ctr: {:.6f}'.format(trn_cnt, trn_impress, trn_clicks, trn_clicks/trn_cnt))
    print('Wrote file:', problem_name + '_tst_unsorted.csv');
    print('Wrote file:', problem_name + '_trn_unsorted.csv');

# Vika, if you are running lots of experiments, it helps to be able to load a
# dataset from a pickle file. It is much faster than reading a CSV file.

# dump the training data using pickle
def dump_tdata_raw (tdata):
    t1 = clock()
    pickle.dump(tdata,open( "tdataraw.p", "wb"))
    t2 = clock()
    print ("Dumped tdata in", format(t2 - t1, "5.3g"), "secs")
    
# load the raw training data
def load_tdata_raw ():
    t1 = clock()
    t = pickle.load( open( "tdataraw.p", "rb" ) )
    t2 = clock()
    print ("Loaded tdata in ", format(t2 - t1, "5.3g"), "secs")
    return t

# utility functions

# print the first 5 items in a list
def first5(l, n=5):
    for item in l[1:5]:
        print(item)

# print the last 5 items in a list
def last5(l, n=5):
    for item in l[len(l)-5:]:
        print(item)

# print the first 5 items in a dict
def dict5(d, n=5):
    i = 0
    for k, v in d.items():
        print (k, v)
        i += 1
        if (i >= n): break


