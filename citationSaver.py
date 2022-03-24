# Import Module
import PyPDF2
import re
import pdfx
from urlextract import URLExtract
import requests
import fitz
import click
import argparse
import os
from urllib.parse import urlparse, ParseResult

#import pdb;pdb.set_trace()

# Parse args
parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-p','--path', help='Localization of the files', default= "./CitationSaver/")
parser.add_argument('-d','--destination', help='Destination of the URLs extract', default= "./URLs/")
parser.add_argument('-a','--afterprocessed', help='Destination of the files processed', default= "./Processed/")
args = vars(parser.parse_args())

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

# Check if the URLs is available
def check_url(scheme, netloc, path, url_parse, output):
    url_parse = ParseResult(scheme, netloc, path, *url_parse[3:])
    response = requests.head(url_parse.geturl())
    if str(response.status_code).startswith("2") or str(response.status_code).startswith("3"):
        output.write(url_parse.geturl()+"\n")
    else:
        url_parse = ParseResult("https", netloc, path, *url_parse[3:])
        response = requests.head(url_parse.geturl())
        if str(response.status_code).startswith("2") or str(response.status_code).startswith("3"):
            output.write(url_parse.geturl()+"\n")  


def processPDFs():

    ##Process input
    mypath = args['path']
    destination = args['destination']
    afterprocessed = args['afterprocessed']
    
    click.secho("Read inputs...", fg='green')
    
    #Check if exists Directory of destination
    if not os.path.exists(destination):
        os.makedirs(destination)

    #Start the process
    click.secho("Starting process documents...", fg='green')
    for subdir, dirs, files in os.walk(mypath):
        if files:
            with click.progressbar(length=len(files), show_pos=True) as progress_bar:
                
                for file in files:

                    progress_bar.update(1)

                    #List with the URLs extracted
                    list_urls = []

                    #Check if the file is a pdf
                    if file.endswith(".pdf"):

                        file_name = os.path.join(subdir, file)

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

                        # CLose the PDF
                        pdfFileObject.close()

                        #Second method: PDFx
                        
                        # Read PDF File
                        pdf = pdfx.PDFx(file_name)
                         
                        # Get list of URL
                        json = pdf.get_references_as_dict()
                        for elem in json['url']:
                            if elem not in list_urls:
                                list_urls.append(elem)

                        #Third method: fitz
                        
                        # Load PDF
                        with fitz.open(file_name) as doc:
                            text = ""
                            for page in doc:
                                text += page.getText().strip()#.replace("\n", "")

                        text = ' '.join(text.split())
                        
                        extract_url(text, list_urls)

                    # Process the URLs 
                    output_file = destination + "output_URLs_" + file.replace(".pdf", "") + ".txt"
                    with open(output_file, 'w') as output:

                        # Remove mailto links
                        links = [url for url in list_urls if "mailto:" not in url]
                        
                        for elem in links:

                            #Remove trash at the end of the URLs
                            if elem.endswith(";") or elem.endswith(".") or elem.endswith(")") or elem.endswith("/"):
                                elem = elem[:-1]

                            url_parse = urlparse(elem, 'http')

                            #URL parse
                            scheme = url_parse.scheme
                            netloc = url_parse.netloc or url_parse.path
                            path = url_parse.path if url_parse.netloc else ''
                            
                            if not netloc.startswith('www.'):
                                netloc = 'www.' + netloc 
                            try:
                                #Check if URL
                                check_url(scheme, netloc, path, url_parse, output)
                            except:
                                continue

                    #Move the processed pdf to a different folder
                    os.system("mv " + filename + " " + afterprocessed)

if __name__ == '__main__':
    processPDFs()