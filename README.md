Python scripts for extracting election data from pdfs (or csv files) and exporting it as a commmon format csv.  

Current organization has a state level directory and then counties. In general, I parse the pdfs for the counties incrementally. First I split the pdf into smallerp pdfs, one for each race, to make processing easier. Then I parse each pdf with camelot to get a pandas dataframe and I output a csv with this format:

election,state,county,precinct,office,candidate,party,vote_mode,votes,writein,result_status,source_url,source_filename,datetime_retrieved

