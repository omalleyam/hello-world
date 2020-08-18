# imports 	
import civis	
client = civis.APIClient()	

# define function	
def import_from_google_sheet(	
    workbook,	
    sheet,	
    schema,	
    table,	
    existing_table_rows,	
    google_remote_host_id=client.credentials.list(type="Google")[0].remote_host_id,	
    civis_remote_host_id=1695,	
):	
    """	
    Inputs:	
        workbook (str): Name of Google Sheets workbook to import from	
        sheet (str): Name of Google Sheets sheet to import from	
        schema (str): Database schema to output to 	
        table (str): Database table (within listed schema) to output to	
        existing_table_rows (str): The behaviour if a table with the requested name already exists. One of “fail”, “truncate”, “append”, or “drop”.Defaults to “fail”.	
        google_remote_host_id (str): "Google Sheet Connection" from Services tab	
        civis_remote_host_id (str): "Redshift cluster" from Services Tab	
    """	

    import civis	
    client = civis.APIClient()	

    database_credential_id = client.default_credential	
    #civis_remote_host_id = 	
    google_credential_id = client.credentials.list(type="Google")[0].id	
    google_remote_host_id = client.credentials.list(type="Google")[0].remote_host_id	

    job = client.imports.post(	
        f"Import from {workbook}.{sheet}",	
        "GdocImport",	
        is_outbound=True,	
        hidden=True,	
        source={	
            "remote_host_id": google_remote_host_id,	
            "credential_id": google_credential_id,	
        },	
        destination={	
            "remote_host_id": civis_remote_host_id,	
            "credential_id": database_credential_id,	
        },	
    )	

    client.imports.post_syncs(	
        job.id,	
        source={	
            "google_worksheet": {"spreadsheet": workbook, "worksheet": sheet,}	
        },	
        destination={	
            "databaseTable": {"schema": schema, "table": table,}	
		},	
	advanced_options={"existing_table_rows": existing_table_rows,},	

    )	
    resp = client.imports.post_runs(job.id)	
    x = civis.futures.CivisFuture(	
        poller=client.imports.get_files_runs,	
        poller_args=(int(job.id), int(resp.run_id)),	
        client=client,	
    )	
    print(x.result()["state"])	

# run function with environment variables 	
#import civis	
#client = civis.APIClient()	
#import_from_google_sheet(os.environ['IMPORT_WORKBOOK_NAME'], os.environ['IMPORT_WORKSHEET_NAME'], os.environ['OUTPUT_SCHEMA_NAME'], os.environ['OUTPUT_TABLE_NAME'], os.environ['OUTPUT_SCHEMA_NAME'])	
import_from_google_sheet('zapier_progressfl_today.csv','zapier_progressfl_today.csv','scratch','progressfl_zapier_import','append')
