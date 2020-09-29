#!/bin/python

from gremlin_python.driver import client, serializer
import argparse
import pandas as pd
import sys
import time


def argument_parse():
    """ """
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--delete', help='cleanup graph DB', action='store_true')
    parser.add_argument('-m', '--mut', help='mutation file called "data_mutations_extended.txt", from cbioportal')
    parser.add_argument('-c', '--clinic', help='clinic file called "data_clinical_sample.txt", from cbioportal')
    args = parser.parse_args()
    delete = args.delete
    mut_file = args.mut
    clinic_file = args.clinic
    return delete, mut_file, clinic_file


def connect_server():
    """Connect to azure cosmos DB.
    
    Connet to azure cosmos DB.
    Need to insert ID, DB name, table name, and key.
    
    Args:
        None

    Returns:
        clt(str): an instance for connecting to azure cosmos DB server
    """

    clt = client.Client(
            'wss://<YOURID>.gremlin.cosmos.azure.com:443/', 'g',
            username="/dbs/<DB_NAME>/colls/<TABLE_NAME>/",
            password="<KEY_VALUE>",
            message_serializer=serializer.GraphSONSerializersV2d0()
            )
    return clt


def cleanup_graph(client):
    """Drop all of graph database.

    Clean up all of graph database.

    Args:
        client(str): an instance for connecting to azure cosmos DB server

    Returns:
        none
    """

    while 1:
        decision = raw_input('\n\tAre you sure clean up all of graph database? Y or N\n')
        print 'decision: ' + decision
        if decision in ['Y', 'y']:
            client.submitAsync("g.V().drop()")
            print("\n\tCleaned up the graph!\n")
            break
        elif decision in ['N', 'n']:
            print("\n\tCancel cleaning up the graph!\n")
            sys.exit()
        else:
            print("\n\tYou need to type Y or N\n")
    return True


def execute_query(client, query_list):
    """Execute query.

    Execute query of add vertex, add edge, etc.

    Args:
        client(str): an instance for connecting to azure cosmos DB server
        query_list(list): a list of query to be executed
    
    Returns:
        none
    """

    for que in query_list:
        print("\n\tRunning this Gremlin query:\n\t{0}\n".format(que))
        callback = client.submitAsync(que)
        if callback.result() is not None:
            print("\n\tInserted this vertex:\n\t{0}\n".format(callback.result().one()))
        else:
            print("\nSomething went wrong with this query: {0}".format(que))
    print("\n")
    return True


def parse_clinic(clinic_file):
    """Parse clinic data file.

    Parse a clinic file called "data_clinical_sample.txt", from cbioportal.

    Args:
        clinic_file(str): a clinic file called "data_clinical_sample.txt", from cbioportal
    
    Returns:
        s_clinic_df(dataframe): a dataframe of clinic file
        cancer_list(list): a list of all cancers in clinic file
    """

    cancer_list = []
    clinic_df = pd.read_csv(clinic_file, sep='\t')
    s_clinic_df = clinic_df.loc[:, [
            '#PATIENT_ID', 'SAMPLE_COLLECTION_SOURCE', 'SPECIMEN_PRESERVATION_TYPE', 'SPECIMEN_TYPE', 'DNA_INPUT',
            'TUMOR_PURITY', 'SAMPLE_TYPE', 'PRIMARY_SITE', 'METASTATIC_SITE', 'SAMPLE_CLASS', 'CANCER_TYPE', 'CANCER_TYPE_DETAILED']
            ]
    cancer_list = list(set(s_clinic_df.loc[:, 'CANCER_TYPE']))
    return s_clinic_df, cancer_list
    

def parse_mut(mut_file):
    """Parse mutaton data file.

    Parse a mutation file called "data_clinical_sample.txt", from cbioportal.

    Args:
        mut_file(str): a mutation file called "data_mutations_extended.txt", from cbioportal

    Returns:
        s_mut_df(dataframe): a dataframe of mutation file
        gene_list(list): a list fo all genes in mutation file
    """

    gene_list = []
    mut_df = pd.read_csv(mut_file, sep='\t')
    s_mut_df = mut_df.loc[:, [
            'Hugo_Symbol', 'NCBI_Build', 'Chromosome', 'Start_Position', 'End_Position', 'Strand', 'Consequence',
            'Variant_Classification', 'Variant_Type', 'Reference_Allele', 'Tumor_Seq_Allele1', 'Tumor_Seq_Allele2', 
            'Tumor_Sample_Barcode', 't_ref_count', 't_alt_count', 'HGVSc', 'HGVSp', 'HGVSp_Short', 'Transcript_ID', 
            'RefSeq', 'Protein_position', 'Codons', 'Hotspot']
            ]
    gene_list = list(set(s_mut_df.loc[:, 'Hugo_Symbol']))
    return s_mut_df, gene_list


def make_clinic_db_query(s_clinic_df, cancer_list):
    """Make a query list for clinic file.

    Make a query list for inserting clinic file into graph database.
    
    Args:
        s_clinic_df(dataframe): a dataframe of clinic file
        cancer_list(list): a list of all cancers in clinic file

    Returns:
        query_list(list): a query list for inserting clinic data into graph database
    """

    query = ''
    check_pid_list = [] #check list for duplication of patient id
    query_list = ["g.addV('CANCER_TYPE').property('data', 'pass').property('id','CANCER_TYPE')"] #root vertex of cancer type
    for cancer in cancer_list:
        query += (
                "g.addV('CANCER').property('data', 'pass').property('id', '{cancer}')\n"
                "g.V('CANCER_TYPE').addE('has').to(g.V('{cancer}'))\n"
                .format(cancer=cancer)
                )
    right_time = time.ctime()
    query += "g.addV('{group}').property('data', 'pass').property('id', '{group}')\n".format(group='MSK-IMPACT') #root vertex of patient
    for row in range(len(s_clinic_df.index)):
        each_row = s_clinic_df.loc[row]
        pid = each_row["#PATIENT_ID"]
        cancer = each_row["CANCER_TYPE"]
        if pid in check_pid_list:
            continue
        else:
            check_pid_list.append(pid)
        query += (
                "g.addV('PATIENT').property('data', 'pass').property('id', '{pid}')\n"
                "g.addV('CLINIC_DATA').property('data', 'pass').property('id', '{pid}_clinic')"
                .format(pid=pid)
                )
        for key, val in zip(s_clinic_df.columns, each_row):
            try:
                if "'" in val:
                    val.replace("'", "prime")
            except:
                pass
            query += ".property('{}', '{}')".format(key, val)
        query += (
                "\ng.V('{group}').addE('has').to(g.V('{pid}'))\n"
                "g.V('{pid}').addE('belong').to(g.V('{group}'))\n"
                "g.V('{pid}').addE('regtime').property('registered_time', '{reg_time}').to(g.V('{pid}_clinic'))\n"
                "g.V('{pid}_clinic').addE('belong').to(g.V('{pid}'))\n"
                "g.V('{pid}_clinic').addE('related').to(g.V('{cancer}'))\n"
                "g.V('{cancer}').addE('of').to(g.V('{pid}_clinic'))\n"
                .format(group='MSK-IMPACT', pid=pid, cancer=cancer, reg_time=right_time)
                )
    query_list.extend([q for q in query.split('\n') if q != ''])
    return query_list


def make_mutation_db_query(s_mut_df, gene_list):
    """Make a query list for mutation file.

    Make a query list for inserting mutation file into graph database.

    Args:
        s_mut_df(dataframe): a dataframe of mutation file
        gene_list(list): a list of all genes in mutation file

    Returns:
        query_list(list): a query list for inserting mutation data into graph database
    """

    query = ''
    check_mut_list = [] #check list for duplication of mutation info
    query_list = ["g.addV('TOTAL_GENE').property('data', 'pass').property('id','TOTAL_GENE')"]
    for gene in gene_list:
        query += (
                "g.addV('GENE').property('data', 'pass').property('id','{gene}')\n"
                "g.V('TOTAL_GENE').addE('has').to(g.V('{gene}'))\n"
                .format(gene=gene)
                )
    for row in range(len(s_mut_df.index)):
        each_row = s_mut_df.loc[row]
        var = '{}_{}_{}_{}'.format(each_row["Chromosome"], each_row["Start_Position"], each_row["Reference_Allele"], each_row["Tumor_Seq_Allele2"])
        var_type = each_row["Variant_Type"]
        pre_pid = each_row["Tumor_Sample_Barcode"]
        pid = '-'.join(pre_pid.split('-')[:-2])
        if pid + '_' + var in check_mut_list:
            continue
        else:
            check_mut_list.append(pid + '_' + var)
        gene = each_row["Hugo_Symbol"]
        query += "g.addV('GENETIC_DATA').property('data', 'pass').property('id', '{pid}_{var}')".format(var=var, pid=pid)
        for key, val in zip(s_mut_df.columns, each_row):
            try:
                if "'" in val:
                    val.replace("'", "prime")
            except:
                pass
            query += ".property('{}', '{}')".format(key, val)
        query += (
                "\ng.V('{pid}').addE('var').to(g.V('{pid}_{var}'))\n"
                "g.V('{pid}_{var}').addE('belong').to(g.V('{pid}'))\n"
                "g.V('{pid}_{var}').addE('related').to(g.V('{gene}'))\n"
                "g.V('{gene}').addE('of').to(g.V('{pid}_{var}'))\n"
                .format(pid=pid, gene=gene, var=var, var_type=var_type)
                )
    query_list.extend([q for q in query.split('\n') if q != ''])
    return query_list


def main():
    """ """

    delete, mut_file, clinic_file = argument_parse()
    clt = connect_server()
    if delete:
        cleanup_graph(clt)
        sys.exit()
    elif mut_file and clinic_file:
        s_clinic_df, cancer_list = parse_clinic(clinic_file)
        clinic_query_list = make_clinic_db_query(s_clinic_df, cancer_list)
        execute_query(clt, clinic_query_list)    
        s_mut_df, gene_list = parse_mut(mut_file)
        mut_query_list = make_mutation_db_query(s_mut_df, gene_list)
        execute_query(clt, mut_query_list)
    else:
        print(
                "\n\tUsage: python {} -d\n\tUsage: python {} -m <mutation file> -c <clinic file>\n"
                .format(sys.argv[0], sys.argv[0])
                )
        sys.exit()


if __name__=="__main__":
    main()
