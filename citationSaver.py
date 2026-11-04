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
import shutil
from urllib.parse import urlparse, ParseResult, urljoin, urldefrag
from fpdf import FPDF
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import subprocess
from bs4 import BeautifulSoup
import time
import sys
import traceback
import socket
import ipaddress
import threading

#import pdb;pdb.set_trace()

# Parse args
parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-p','--path', help='Localization of the files', default= "./CitationSaver/")
parser.add_argument('-t','--tika', help='Localization of the Tika Jar', default= "tika-app-1.24.1.jar")
parser.add_argument('-l','--linkextract', help='Localization of the tikalinkextract binary', default= "/opt/citationSaver/tikalinkextract-linux64")
parser.add_argument('-tu','--tikaurl', help='URL of the Tika server used by tikalinkextract', default= "http://127.0.0.1:9998/")
parser.add_argument('-tw','--tikawait', help='Seconds to wait for the Tika server to start', type=int, default= 120)
parser.add_argument('-ds','--downloadsize', help='Maximum size in MB of a document downloaded from a link', type=int, default= 200)
parser.add_argument('-dt','--downloadtime', help='Maximum time in seconds to download a document from a link', type=int, default= 300)
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

#Path tikalinkextract binary
path_linkextract = args['linkextract']

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

def extract_urls_pdf(file, file_name):

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

    #Fourth method: tika

    # Load PDF
    #The method works, but it is slow and the extractor can be unreliable

    #process = Popen(['java', '-jar', path_tika, '-t', file_name], stdout=PIPE, stderr=PIPE)
    #result = process.communicate()
    #extract_url(result[0].decode('utf-8'), list_urls)

    #Fifth method: tika from DSpace Link Extractor (https://github.com/arquivo/dspace-link-extractor)

    #Waiting Time for each request
    time.sleep(2)

    #Beware of the file's permissions (tikalinkextract-linux64)
    try:
        result = subprocess.run([path_linkextract, "-seeds", "-file", file_name], capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=600)
    except (OSError, subprocess.TimeoutExpired) as e:
        #Update GoogleSheet
        update_google_sheet(file, "-", "Problem using tikalinkextract: " + str(e), True)
        return None

    if result.returncode != 0:
        #Update GoogleSheet
        update_google_sheet(file, "-", "Problem using tikalinkextract: " + result.stderr.strip(), True)
        return None

    # Strip the newline characters from each line
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]

def extract_urls_txt(file, file_name, text):

    #tikalinkextract reads PDFs, so write the text to a temporary PDF next to the file
    pdf = FPDF()
    # Add a page
    pdf.add_page()
    # set style and size of font
    # that you want in the pdf
    pdf.set_font("Arial", size = 15)

    # insert the texts in pdf
    for x in text.splitlines():
        pdf.cell(200, 10, txt = x, ln = 1, align = 'C')

    # save the pdf with name .pdf
    pdf_name = os.path.splitext(file_name)[0] + ".pdf"
    pdf.output(pdf_name)

    try:
        #Extract URLs using tikalinkextract
        return extract_urls_pdf(file, pdf_name)
    finally:
        #Remove the temporary pdf
        os.remove(pdf_name)

def extract_urls_html(html, base_url=""):

    list_urls = []

    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find all anchor tags (links) on the page
    for link in soup.find_all('a'):
        href = link.get('href')
        if href:
            try:
                # Transform relative URL to absolute (only possible when the page address is known) and drop the #fragment
                url = urldefrag(urljoin(base_url, href.strip()))[0]

                #Keep only web links (skip javascript:, tel:, mailto:, relative links without base, ...) and skip the page itself
                if urlparse(url).scheme not in ("http", "https") or url == urldefrag(base_url)[0]:
                    continue
            except ValueError:
                #Skip links that are not valid URLs, e.g. http://[oops
                continue

            if url not in list_urls:
                list_urls.append(url)

    return list_urls

def clean_url(url):

    #Remove trailing punctuation picked up from the surrounding text
    while url.endswith((";", ".", ")")):

        #Keep closing brackets that are part of the URL, e.g. wiki/Foo_(bar)
        if url.endswith(")") and url.count("(") >= url.count(")"):
            break

        url = url[:-1]

    return url

def check_urls(list_urls, output_file):

    # Remove mailto links
    links = [url for url in list_urls if "mailto:" not in url]

    #Always create the file, even when there are no URLs
    with open(output_file, 'w') as output:

        for elem in links:

            #Remove trash at the end of the URLs
            elem = clean_url(elem)

            output.write(elem+"\n")

    #Number of URLs written
    return len(links)

def update_google_sheet(file, path_output, note, error):
    
    #Get the index from the file being processed in the google sheet
    index = df.index[df['File Name CitationSaver System']==file].tolist()

    #Skip files without a row in the google sheet
    if not index:
        click.secho("No row in the Google Sheet for " + file, fg='yellow')
        return

    if not error:

        #Check if columns are empty for the present row
        if pd.isnull(df.at[index[0], 'Results URLs File Path']):
                
                #Update value Google Sheet
                df.at[index[0], 'Results URLs File Path'] = path_output

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

        #Update value Google Sheet
        df.at[index[0], 'Results URLs File Path'] = path_output
        df.at[index[0], 'Note/Error'] = note

def wait_for_tika(url, timeout):

    #The Tika server may still be starting, so try again until the timeout
    deadline = time.time() + timeout
    while True:
        try:
            requests.get(url, timeout=5)
            return True
        except requests.RequestException:
            if time.time() >= deadline:
                return False
            time.sleep(2)

def check_public_url(url):

    #Refuse links to localhost, private networks, cloud metadata, ... (every address of the host must be public)
    try:
        host = urlparse(url).hostname
    except ValueError as e:
        raise requests.exceptions.InvalidURL(str(e))
    if not host:
        raise requests.exceptions.InvalidURL("No host in " + url)

    try:
        addresses = [info[4][0] for info in socket.getaddrinfo(host, None)]
    except socket.gaierror as e:
        raise requests.exceptions.ConnectionError("Could not resolve " + host + ": " + str(e))

    for address in addresses:
        ip = ipaddress.ip_address(address.split('%')[0])
        if not ip.is_global or ip.is_multicast:
            raise requests.exceptions.InvalidURL("The link points to a non-public address (" + host + " -> " + address + ")")

def stop_download(response):

    #Shut the connection down, so a read waiting for data returns at once
    try:
        response.raw.connection.sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        response.close()

def download_link(url):

    #Limits for the downloaded document (size in MB, time in seconds)
    max_size = args['downloadsize'] * 1024 * 1024
    deadline = time.time() + args['downloadtime']

    #Follow the redirects here, so that every address is checked before it is requested
    for _ in range(10):
        check_public_url(url)
        response = requests.get(url, timeout=30, allow_redirects=False, stream=True)
        if response.is_redirect:
            response.close()
            url = urljoin(url, response.headers['location'])
            continue

        #Refuse early when the server already says the document is too big
        length = response.headers.get('content-length', '')
        if length.isdigit() and int(length) > max_size:
            response.close()
            raise requests.RequestException("The document is larger than " + str(args['downloadsize']) + " MB")

        #The timeout of requests is per read only, so a server sending a byte now and then is cut by a timer at the deadline
        timer = threading.Timer(max(deadline - time.time(), 0), stop_download, [response])
        timer.daemon = True
        timer.start()

        #Read the body in chunks, so a huge download is stopped
        content = bytearray()
        try:
            for chunk in response.iter_content(64 * 1024):
                content += chunk
                if len(content) > max_size:
                    raise requests.RequestException("The document is larger than " + str(args['downloadsize']) + " MB")
        except Exception:
            if time.time() < deadline:
                raise
        finally:
            timer.cancel()
            response.close()
        if time.time() >= deadline:
            raise requests.exceptions.Timeout("The download took longer than " + str(args['downloadtime']) + " seconds")

        #Keep the body in the response, so response.content and response.text work as usual
        response._content = bytes(content)
        return response

    raise requests.exceptions.TooManyRedirects("More than 10 redirects")

def process_file(subdir, file, destination, afterprocessed):

    #List with the URLs extracted
    list_urls = []

    #Complete file path name
    file_name = os.path.join(subdir, file)

    #Leave files without a row in the google sheet for the next run (the web app adds the row after saving the file)
    if df[df['File Name CitationSaver System']==file].empty:
        click.secho("No row in the Google Sheet yet for " + file + ", left for the next run", fg='yellow')
        return

    #Check if the file is a pdf
    if file.endswith(".pdf"):

        #Check if the pdf is well formed
        if check_pdf(file_name, file):

            #Extract URLs using PyPDF2, PDFx, fitz
            list_urls = extract_urls_pdf(file, file_name)

            #Check if the extraction worked
            if list_urls is not None:

                output_file = destination + "output_URLs_" + os.path.splitext(file)[0] + ".txt"

                #Check if the URLs are correct and write in a file
                number_urls = check_urls(list_urls, output_file)

                #Update GoogleSheet
                update_google_sheet(file, output_file, "" if number_urls else "No URLs found", False)

        else:
            #Update GoogleSheet
            update_google_sheet(file, "-", "The PDF download is not in the correct form", True)

        #Move the processed pdf to a different folder
        shutil.move(file_name, os.path.join(afterprocessed, file))

    #Check if the file is a txt
    elif file.endswith(".txt"):

        # open the text file in read mode (ISO-8859-1 never fails and is the encoding of the pdf font)
        with open(file_name, "r", encoding='ISO-8859-1') as f:
            text = f.read()

        #Extract URLs using tikalinkextract
        list_urls = extract_urls_txt(file, file_name, text)

        #Check if the extraction worked
        if list_urls is not None:

            output_file = destination + "output_URLs_" + os.path.splitext(file)[0] + ".txt"

            #Check if the URLs are correct and write in a file
            number_urls = check_urls(list_urls, output_file)

            #Update GoogleSheet
            update_google_sheet(file, output_file, "" if number_urls else "No URLs found", False)

        #Move the processed txt to a different folder
        shutil.move(file_name, os.path.join(afterprocessed, file))

    #Check if the file is a html page
    elif file.endswith(".html"):

        #Read the bytes and let BeautifulSoup detect the encoding
        with open(file_name, "rb") as file_html:
            html = file_html.read()

        #Extract the links of the page (an uploaded page has no address, so relative links are lost)
        list_urls = extract_urls_html(html)

        output_file = destination + "output_URLs_" + os.path.splitext(file)[0] + ".txt"

        #Check if the URLs are correct and write in a file
        number_urls = check_urls(list_urls, output_file)

        #Update GoogleSheet
        update_google_sheet(file, output_file, "" if number_urls else "No URLs found", False)

        #Move the processed html to a different folder
        shutil.move(file_name, os.path.join(afterprocessed, file))

    #Check if the file is a link
    elif file.endswith(".link"):

        #Open the file
        with open(file_name, "r") as file_link:
            lines = file_link.readlines()

        if len(lines) == 1:

            #Get the first row
            first_line = lines[0].strip()

            #The form accepts links without the scheme (e.g. publico.pt/artigo.pdf): add https:// as the web app does
            if not first_line.lower().startswith(("http://", "https://")):
                first_line = "https://" + first_line

            #Request the content
            try:
                response = download_link(first_line)
            except requests.RequestException as e:
                #Update GoogleSheet
                update_google_sheet(file, "-", "The link could not be downloaded: " + str(e), True)
                return

            if response.status_code == 200:

                #Get the type of document downloaded
                content_type = response.headers.get('content-type', '').lower()

                #Sanity Check
                if content_type.startswith("application/pdf"):

                    #Create a new PDF file with the response content
                    file_output = os.path.join(subdir, file.replace(".link", ".pdf"))
                    with open(file_output, "wb") as output:
                        output.write(response.content)

                    #Remove the processed .link file
                    os.remove(file_name)

                    ###Same process as for PDFs files

                    #Check if the pdf is well formed
                    if check_pdf(file_output, file):

                        #Extract URLs using PyPDF2, PDFx, fitz
                        list_urls = extract_urls_pdf(file, file_output)

                        #Check if the extraction worked
                        if list_urls is not None:

                            output_file = destination + "output_URLs_" + os.path.splitext(file)[0] + ".txt"

                            #Check if the URLs are correct and write in a file
                            number_urls = check_urls(list_urls, output_file)

                            #Update GoogleSheet
                            update_google_sheet(file, output_file, "" if number_urls else "No URLs found", False)

                    else:
                        #Update GoogleSheet
                        update_google_sheet(file, "-", "The PDF download is not in the correct form", True)

                    #Move the processed pdf to a different folder
                    shutil.move(file_output, os.path.join(afterprocessed, os.path.basename(file_output)))

                elif content_type.startswith("text/html"):

                    #Example: https://www.spinellis.gr/sw/url-decay/

                    #Extract the links of the page
                    list_urls = extract_urls_html(response.text, first_line)

                    output_file = destination + "output_URLs_" + os.path.splitext(file)[0] + ".txt"

                    #Check if the URLs are correct and write in a file
                    number_urls = check_urls(list_urls, output_file)

                    #Update GoogleSheet
                    update_google_sheet(file, output_file, "" if number_urls else "No URLs found", False)

                    #Move the processed pdf to a different folder
                    shutil.move(file_name, os.path.join(afterprocessed, file))

                elif content_type.startswith("text/plain"):

                    ###Same process as for TXT files

                    #Extract URLs using tikalinkextract
                    list_urls = extract_urls_txt(file, file_name, response.content.decode('ISO-8859-1'))

                    #Check if the extraction worked
                    if list_urls is not None:

                        output_file = destination + "output_URLs_" + os.path.splitext(file)[0] + ".txt"

                        #Check if the URLs are correct and write in a file
                        number_urls = check_urls(list_urls, output_file)

                        #Update GoogleSheet
                        update_google_sheet(file, output_file, "" if number_urls else "No URLs found", False)

                    #Move the processed link to a different folder
                    shutil.move(file_name, os.path.join(afterprocessed, file))

                else:
                    #Update GoogleSheet
                    update_google_sheet(file, "-", "The document download is not a PDF, HTML or TXT document (" + content_type + ")", True)
            else:
                #Update GoogleSheet
                update_google_sheet(file, "-", "The link is not 200", True)
        else:
            #Update GoogleSheet
            update_google_sheet(file, "-", "The link have more than one line", True)
    else:
        #Update GoogleSheet
        update_google_sheet(file, "-", "Wrong File", True)

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

                    try:
                        process_file(subdir, file, destination, afterprocessed)
                    except Exception as e:
                        #One bad file must not stop the processing of the other files
                        click.secho("Unexpected error processing " + file + ":\n" + traceback.format_exc(), fg='red')
                        update_google_sheet(file, "-", "Unexpected error: " + repr(e), True)

if __name__ == '__main__':

    #confirm the permissions
    #Beware of the file's permissions (tikalinkextract-linux64)
    try:
        os.chmod(path_linkextract, 0o755)
    except OSError as e:
        click.secho("Could not set permissions on tikalinkextract: " + str(e), fg='yellow')

    #tikalinkextract needs the Tika server: stop before touching any file if it is not up
    click.secho("Waiting for the Tika server at " + args['tikaurl'] + "...", fg='green')
    if not wait_for_tika(args['tikaurl'], args['tikawait']):
        click.secho("The Tika server is not available. No file was processed.", fg='red')
        sys.exit(1)

    #Process
    try:
        processCitationSaver()
    finally:
        #Update the google sheet, even if the process stopped midway
        set_with_dataframe(worksheet, df)

 
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
