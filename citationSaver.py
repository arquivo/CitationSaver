# Import Module
import PyPDF2
from PyPDF2.utils import PdfReadError
import pdfx
from urlextract import URLExtract
import requests
import fitz
import click
import argparse
import os
from urllib.parse import urlparse, ParseResult, urljoin
from fpdf import FPDF
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from subprocess import PIPE, Popen
from bs4 import BeautifulSoup

#import pdb;pdb.set_trace()

# Parse args
parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-p','--path', help='Localization of the files', default= "./CitationSaver/")
parser.add_argument('-t','--tika', help='Localization of the Tika Jar', default= "tika-app-1.24.1.jar")
parser.add_argument('-d','--destination', help='Destination of the URLs extract', default= "./URLs/")
parser.add_argument('-a','--afterprocessed', help='Destination of the files processed', default= "./Processed/")
parser.add_argument('-w','--pathwarc', help='Destination of the WARCs for each file', default= "./WARCs/")
parser.add_argument('-j','--pathjson', help='Destination of the json file with google service key', default= "JSON")
parser.add_argument('-k','--key', help='Key Google Spreadsheet', default= "KEY")
parser.add_argument('-ws','--worksheet', help='Worksheet Google Spreadsheet', default= "WORKSHEET")
args = vars(parser.parse_args())

#Connect gspread
gc = gspread.service_account(filename=args['pathjson'])
sh =  gc.open_by_key(args['key'])
worksheet = sh.worksheet(args['worksheet'])

#Transform worksheet to pandas dataframe
df = get_as_dataframe(worksheet)

#Path Tika Jar
path_tika = args['tika']

#Global variable with the URLs check for each document
#list_urls_check = []

# Extract URLs from text
def extract_url(text, list_urls):
    extractor = URLExtract()
    urls = extractor.find_urls(text)
    for url in urls:
        url = url.replace(",", "")
        if "http" in url:
            url = url[url.find('http'):]
        if url not in list_urls:
            list_urls.append(url)

def check_pdf(file_name, file):
    try:
        pdf = PyPDF2.PdfFileReader(file_name)
        return True
    except PdfReadError:
        return False

def extract_urls_pdf(file, file_name, list_urls):

    """
    PyPDF2 has problems processing pdfs. The words stay together. 
    PyPDF2 will be deactivated for now.
    """
    """
    #First method: PyPDF2

    # Open File file
    pdfFileObject = open(file_name, 'rb')
      
    pdfReader = PyPDF2.PdfFileReader(pdfFileObject)

    # Iterate through all pages
    for page_number in range(pdfReader.numPages):
         
        pageObject = pdfReader.getPage(page_number)
        
        # Extract text from page
        pdf_text = pageObject.extractText()
        
        extract_url(pdf_text, list_urls)

    if not list_urls:
        #Update GoogleSheet
        update_google_sheet(file, "", "", "", "Problem using PyPDF2 process", True)

    # CLose the PDF
    pdfFileObject.close()
    """
    """
    #Second method: PDFx
    
    # Read PDF File
    pdf = pdfx.PDFx(file_name)
    
    # Get list of URL
    json = pdf.get_references_as_dict()
    if len(json) != 0:
        for elem in json['url']:
            if elem not in list_urls:
                list_urls.append(elem)
    else:
        #Update GoogleSheet
        update_google_sheet(file, "", "", "", "Problem using PDFx process", True)

    #Third method: fitz

    # Load PDF
    with fitz.open(file_name) as doc:
        text = ""
        for page in doc:
            text += page.getText().strip()#.replace("\n", "")

    text = ' '.join(text.split())
    
    extract_url(text, list_urls)
    """

    #Third method: tika

    # Load PDF
    #process = Popen(['java', '-jar', path_tika, '-t', file_name], stdout=PIPE, stderr=PIPE)
    #result = process.communicate()
    #extract_url(result[0].decode('utf-8'), list_urls)

    """
    Extract the URLs from the pdf
    """

    #Waiting Time for each request
    #time.sleep(2)

    #TODO - Getting a mechanism to catch errors from tikalinkextract-linux64 script
    #Beware of the file's permissions (tikalinkextract-linux64)
    os.system("/opt/citationSaver/tikalinkextract-linux64 -seeds -file "+ file_name +" >> ./trash.txt")

    # Open the file in read mode
    with open('./trash.txt', 'r') as file:
        # Read all lines from the file into a list
        lines = file.readlines()
    
    # Strip the newline characters from each line
    list_urls = [line.strip() for line in lines]

    #os.system("rm -rf ./trash.txt")

def check_urls(list_urls, output_file, list_urls_check):
 
    if list_urls != []:
        # Process the URLs 
        
        with open(output_file, 'w') as output:

            # Remove mailto links
            links = [url for url in list_urls if "mailto:" not in url]
            
            for elem in links:

                #Remove trash at the end of the URLs
                if elem.endswith(";") or elem.endswith(".") or elem.endswith(")") or elem.endswith("/"):
                    elem = elem[:-1]

                url_parse = urlparse(elem, 'http')

                netloc = url_parse.netloc or url_parse.path
                
                if not netloc.startswith('www.'):
                    netloc = 'www.' + netloc 

                if netloc.lower() not in list_urls_check:
                    output.write(netloc.lower()+"\n")
                    list_urls_check.append(netloc)

    return list_urls_check

def update_google_sheet(file, path_output, list_urls, list_urls_check, note, error):
    
    #Get the index from the file being processed in the google sheet
    index = df.index[df['File Name CitationSaver System']==file].tolist()

    if not error:

        #Check if columns are empty for the present row
        if pd.isnull(df.at[index[0], 'Results URLs File Path']) and pd.isnull(df.at[index[0], 'Results URLs without check']) and pd.isnull(df.at[index[0], 'Results URLs domain']):
                
                #Update value Google Sheet
                df.at[index[0], 'Results URLs File Path'] = path_output
                df.at[index[0], 'Results URLs without check'] = list_urls
                df.at[index[0], 'Results URLs domain'] = list_urls_check
                if note != "":
                    if not pd.isnull(df.at[index[0], 'Note/Error']):
                        df.at[index[0], 'Note/Error'] = str(df.at[index[0], 'Note/Error']) + " " + note
                    else:
                        df.at[index[0], 'Note/Error'] = note
    
        else:
            #Put an extra note with this problem
            if not pd.isnull(df.at[index[0], 'Note/Error']) and "The script is processing the same document over and over again" not in df.at[index[0], 'Note/Error']:
                df.at[index[0], 'Note/Error'] = str(df.at[index[0], 'Note/Error']) + "; The script is processing the same document over and over again"
            else:
                df.at[index[0], 'Note/Error'] = "The script is processing the same document over and over again"
    
    else:
        
        if path_output == "-" and list_urls == "-" and list_urls_check == "-":
            
            #Update value Google Sheet
            df.at[index[0], 'Results URLs File Path'] = path_output
            df.at[index[0], 'Results URLs without check'] = list_urls
            df.at[index[0], 'Results URLs domain'] = list_urls_check
            df.at[index[0], 'Note/Error'] = note
        
        else:
            if not pd.isnull(df.at[index[0], 'Note/Error']):
                df.at[index[0], 'Note/Error'] = str(df.at[index[0], 'Note/Error']) + " " + note
            else:
                df.at[index[0], 'Note/Error'] = note

def processCitationSaver():

    click.secho("Read inputs...", fg='green')

    ##Process input
    mypath = args['path']
    destination = args['destination']
    afterprocessed = args['afterprocessed']
    
    #Check if exists Directory of destination
    if not os.path.exists(destination):
        os.makedirs(destination)

    #Check if exists Directory of afterprocessed
    if not os.path.exists(afterprocessed):
        os.makedirs(afterprocessed)

    #Start the process
    click.secho("Starting process documents...", fg='green')

    #Iterate through the files inside the folder
    for subdir, dirs, files in os.walk(mypath):
        
        #Check if the directory is not empty
        if files:

            #Check the progress
            with click.progressbar(length=len(files), show_pos=True) as progress_bar:
                
                #For each file
                for file in files:

                    progress_bar.update(1)

                    #List with the URLs extracted
                    list_urls = []
                    list_urls_check = []

                    #Complete file path name
                    file_name = os.path.join(subdir, file)

                    #Check if the file is a pdf
                    if file.endswith(".pdf"):

                        #Check if the pdf is well formed
                        if check_pdf(file_name, file):
                            
                            #Extract URLs using PyPDF2, PDFx, fitz
                            extract_urls_pdf(file, file_name, list_urls)

                            output_file = destination + "output_URLs_" + file.replace(".pdf", "") + ".txt"

                            #Check if the URLs are correct and write in a file
                            list_urls_check = check_urls(list_urls, output_file, list_urls_check)

                            #Update GoogleSheet
                            update_google_sheet(file, output_file, list_urls, list_urls_check, "", False)

                        else:
                            #Update GoogleSheet
                            update_google_sheet(file, "-", "-", "-", "The PDF download is not in the correct form", True)

                        #Move the processed pdf to a different folder
                        os.system("mv " + file_name + " " + afterprocessed)

                    #Check if the file is a txt
                    elif file.endswith(".txt"):

                        pdf = FPDF()
                        # Add a page
                        pdf.add_page()
                        # set style and size of font 
                        # that you want in the pdf
                        pdf.set_font("Arial", size = 15)

                        # open the text file in read mode
                        f = open(file_name, "r", encoding='ISO-8859-1')
                          
                        # insert the texts in pdf
                        for x in f:
                            pdf.cell(200, 10, txt = x, ln = 1, align = 'C')
                           
                        # save the pdf with name .pdf
                        pdf.output(file_name.replace(".txt", ".pdf"))

                        #Move the processed pdf to a different folder
                        os.system("mv " + file_name + " " + afterprocessed)

                        #Extract URLs using PyPDF2, PDFx, fitz
                        extract_urls_pdf(file, file_name.replace(".txt", ".pdf"), list_urls)

                        output_file = destination + "output_URLs_" + file.replace(".pdf", "") + ".txt"

                        #Check if the URLs are correct and write in a file
                        list_urls_check = check_urls(list_urls, output_file, list_urls_check)

                        #Update GoogleSheet
                        update_google_sheet(file, output_file, list_urls, list_urls_check, "", False)

                        #Move the processed pdf to a different folder
                        os.system("rm -rf " + file_name.replace(".txt", ".pdf"))

                    #Check if the file is a link
                    elif file.endswith(".link"):
                        
                        #Open the file
                        file_link = open(file_name, "r")

                        lines = file_link.readlines()
                        
                        if len(lines) == 1:

                            #Get the first row
                            first_line = lines[0]

                            #Request the content
                            response = requests.get(first_line)
                            
                            if response.status_code == 200:

                                #Get the type of document downloaded
                                content_type = response.headers.get('content-type')

                                #Sanity Check
                                if content_type == "application/pdf":

                                    #Create a new PDF file with the response content
                                    file_output = os.path.join(subdir, file.replace(".link", ".pdf"))
                                    output = open(file_output, "wb")
                                    output.write(response.content)

                                    #Remove the processed .link file
                                    os.system("rm -rf " + file_name)

                                    ###Same process as for PDFs files

                                    #Check if the pdf is well formed
                                    if check_pdf(file_output, file):
                                    
                                        #Extract URLs using PyPDF2, PDFx, fitz
                                        extract_urls_pdf(file, file_output, list_urls)

                                        output_file = destination + "output_URLs_" + file.replace(".link", ".txt")

                                        #Check if the URLs are correct and write in a file
                                        list_urls_check = check_urls(list_urls, output_file, list_urls_check)

                                        #Update GoogleSheet
                                        update_google_sheet(file, output_file, list_urls, list_urls_check, "", False)
                                    
                                    else:
                                        #Update GoogleSheet
                                        update_google_sheet(file, "-", "-", "-", "The PDF download is not in the correct form", True)

                                    #Move the processed pdf to a different folder
                                    os.system("mv " + file_output + " " + afterprocessed)

                                elif content_type == "text/html":
                                    
                                    #Example: https://www.spinellis.gr/sw/url-decay/

                                    # Parse the HTML content of the page using BeautifulSoup
                                    soup = BeautifulSoup(response.text, 'html.parser')
                                    
                                    # Find all anchor tags (links) on the page
                                    links = soup.find_all('a')
                                    
                                    # Extract and print the absolute URLs of each link
                                    for link in links:
                                        href = link.get('href')
                                        if href:
                                            list_urls.append(urljoin(base_url, href))  # Transform relative URL to absolute

                                    output_file = destination + "output_URLs_" + file.replace(".link", ".txt")

                                    #Check if the URLs are correct and write in a file
                                    list_urls_check = check_urls(list_urls, output_file, list_urls_check)

                                    #Update GoogleSheet
                                    update_google_sheet(file, output_file, list_urls, list_urls_check, "", False)

                                else:
                                    #Update GoogleSheet
                                    update_google_sheet(file, "-", "-", "-", "The document download is not application/pdf", True)
                            else:
                                #Update GoogleSheet
                                update_google_sheet(file, "-", "-", "-", "The link is not 200", True)
                        else:
                            #Update GoogleSheet
                            update_google_sheet(file, "-", "-", "-", "The link have more than one line", True)
                    else:
                        #Update GoogleSheet
                        update_google_sheet(file, "-", "-", "-", "Wrong File", True)
    
    #Update the google sheet
    set_with_dataframe(worksheet, df)

if __name__ == '__main__':
    processCitationSaver()

 
"""
###########################
###Save URLs to WARC using wget
###Will be not use in this first stage
###########################
pathwarc = args['pathwarc']
#Check if exists Directory of pathwarc
if not os.path.exists(pathwarc):
    os.makedirs(pathwarc)
#Start the process
click.secho("Starting crawling...", fg='green')
for subdir, dirs, files in os.walk(destination):
    if files:
        with click.progressbar(length=len(files), show_pos=True) as progress_bar:
            
            for file in files:
                progress_bar.update(1)
                file_URLs = os.path.join(subdir, file)
                if file.endswith(".pdf"):
                    file_warc = pathwarc + file.replace(".txt", "")
                    
                    os.system("wget --page-requisites --span-hosts --convert-links  --execute robots=off --adjust-extension --no-directories --directory-prefix=output --warc-cdx --warc-file=" + file_warc + " --wait=0.1 --user-agent=\"Arquivo-web-crawler (compatible; CitationSaver +https://arquivo.pt/faq-crawling)\" -i " + file_URLs)
                    cdx_file = pathwarc + file.replace(".txt", "") + ".cdx"
                    os.system("rm -rf " + cdx_file)
"""
