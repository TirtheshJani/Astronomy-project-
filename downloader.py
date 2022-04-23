# ---------------------------------------------------------#
#   astroNN.apogee.downloader: download apogee files
# ---------------------------------------------------------#

import getpass
import os
import urllib.request, urllib.error
import warnings

import numpy as np
from astroNN.apogee.apogee_shared import apogee_env, apogee_default_dr
from astroNN.shared.downloader_tools import TqdmUpTo, filehash
from astropy.io import fits
from astroNN.shared import logging as logging

currentdir = os.getcwd()

# global var
warning_flag = False
_ALLSTAR_TEMP = {}
__apogee_credentials_username = None
__apogee_credentials_pw = None


def __apogee_credentials_downloader(url, fullfilename):
    """
    Download file at the URL with apogee credentials, this function will prompt for username and password
    :param url: URL
    :type url: str
    :param fullfilename: Full file name including path in local system
    :type fullfilename: str
    :return: None
    :History: 2018-Aug-31 - Written - Henry Leung (University of Toronto)
    """
    passman = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    global __apogee_credentials_username
    global __apogee_credentials_pw
    if __apogee_credentials_username is None:
        print(
            "\nYou are trying to access APOGEE proprietary data...Please provide username and password..."
        )
        __apogee_credentials_username = input("Username: ")
        __apogee_credentials_pw = getpass.getpass("Password: ")
    passman.add_password(
        None, url, __apogee_credentials_username, __apogee_credentials_pw
    )
    authhandler = urllib.request.HTTPBasicAuthHandler(passman)
    opener = urllib.request.build_opener(authhandler)
    urllib.request.install_opener(opener)
    # Check if directory exists
    if not os.path.exists(os.path.dirname(fullfilename)):
        os.makedirs(os.path.dirname(fullfilename))
    try:
        with TqdmUpTo(
            unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]
        ) as t:
            urllib.request.urlretrieve(url, fullfilename, reporthook=t.update_to)
    except urllib.error.HTTPError as emsg:
        if "401" in str(emsg):
            __apogee_credentials_username = None
            __apogee_credentials_pw = None
            raise ConnectionError("Wrong username or password")
        elif "404" in str(emsg):
            warnings.warn(f"{url} cannot be found on server, skipped")
            fullfilename = warning_flag
        else:
            # don't raise error, so a batch downloading script will keep running despite some files not found
            warnings.warn(f"Unknown error occurred - {emsg}", RuntimeWarning)
            fullfilename = warning_flag

    return fullfilename


def allstar(dr=None, flag=None):
    """
    Download the allStar file (catalog of ASPCAP stellar parameters and abundances from combined spectra)
    :param dr: APOGEE DR
    :type dr: int
    :param flag: 0: normal, 1: force to re-download
    :type flag: int
    :return: full file path and download in background if not found locally, False if cannot be found on server
    :rtype: str
    :History: 2017-Oct-09 - Written - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

    if dr == 17:
        file_hash = "0e70c02323132af4045545d2329e3f1cb8fdb1e0"

        fullfoldername = os.path.join(os.sep,
            apogee_env(), "dr17/apogee/spectro/aspcap/dr17/synspec/"
        )
        # Check if directory exists
        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)
        filename = "allStar-dr17-synspec.fits"
        fullfilename = os.path.join(os.sep,fullfoldername, filename)
        url = f"https://data.sdss.org/sas/dr17/apogee/spectro/aspcap/dr17/synspec/{filename}"
    else:
        raise ValueError("allstar() only supports APOGEE DR13-DR17")

    # check file integrity
    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash.lower():
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            allstar(dr=dr, flag=1)
        else:
            logging.info(fullfilename + " was found!")

    # Check if files exists
    if not os.path.isfile(os.path.join(os.sep,fullfoldername, filename)) or flag == 1:
        with TqdmUpTo(
            unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]
        ) as t:
            try:
                urllib.request.urlretrieve(url, fullfilename, reporthook=t.update_to)
                logging.info(
                    f"Downloaded DR{dr:d} allStar file catalog successfully to {fullfilename}"
                )
                checksum = filehash(fullfilename, algorithm="sha1")
                if checksum != file_hash.lower():
                    warnings.warn(
                        "File corruption detected, astroNN is attempting to download again"
                    )
                    allstar(dr=dr, flag=1)
            except urllib.error.HTTPError as emsg:
                if "401" in str(emsg):
                    fullfilename = __apogee_credentials_downloader(url, fullfilename)
                elif "404" in str(emsg):
                    warnings.warn(f"{url} cannot be found on server, skipped")
                    fullfilename = warning_flag
                else:
                    warnings.warn(f"Unknown error occurred - {emsg}")
                    fullfilename = warning_flag

    return fullfilename


def apogee_astronn(dr=None, flag=None):
    """
    Download the apogee_astroNN file (catalog of astroNN stellar parameters, abundances, distances and orbital
     parameters from combined spectra)
    :param dr: APOGEE DR
    :type dr: int
    :param flag: 0: normal, 1: force to re-download
    :type flag: int
    :return: full file path and download in background if not found locally, False if cannot be found on server
    :rtype: str
    :History: 2019-Dec-10 - Written - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

    if dr == 17:
        # Check if directory exists
        fullfoldername = os.path.join(os.sep,apogee_env(), "dr17/apogee/vac/apogee-astronn/")
        # Check if directory exists
        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)
        filename = "apogee_astroNN-DR17.fits"
        fullfilename = os.path.join(os.sep,fullfoldername, filename)
        file_hash = "c422b9adba840b3415af2fe6dec6500219f1b68f"

        url = f"https://data.sdss.org/sas/dr17/apogee/vac/apogee-astronn/{filename}"
    else:
        raise ValueError("apogee_astroNN() only supports APOGEE DR16-DR17")

    # check file integrity
    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash.lower():
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            apogee_astronn(dr=dr, flag=1)
        else:
            logging.info(fullfilename + " was found!")

    # Check if files exists
    if not os.path.isfile(os.path.join(os.sep,fullfoldername, filename)) or flag == 1:
        with TqdmUpTo(
            unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]
        ) as t:
            try:
                urllib.request.urlretrieve(url, fullfilename, reporthook=t.update_to)
                logging.info(
                    f"Downloaded DR{dr:d} apogee_astroNN file catalog successfully to {fullfilename}"
                )
                checksum = filehash(fullfilename, algorithm="sha1")
                if checksum != file_hash.lower():
                    warnings.warn(
                        "File corruption detected, astroNN is attempting to download again"
                    )
                    apogee_astronn(dr=dr, flag=1)
            except urllib.error.HTTPError as emsg:
                if "401" in str(emsg):
                    fullfilename = __apogee_credentials_downloader(url, fullfilename)
                elif "404" in str(emsg):
                    warnings.warn(f"{url} cannot be found on server, skipped")
                    fullfilename = warning_flag
                else:
                    warnings.warn(f"Unknown error occurred - {emsg}")
                    fullfilename = warning_flag

    return fullfilename




def allvisit(dr=None, flag=None):
    """
    Download the allVisit file (catalog of properties from individual visit spectra)
    :param dr: APOGEE DR
    :type dr: int
    :param flag: 0: normal, 1: force to re-download
    :type flag: int
    :return: full file path and download in background if not found locally, False if cannot be found on server
    :rtype: str
    :History: 2017-Oct-11 - Written - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

    
    if dr == 17:
        file_hash = "fb2f5ecbabbe156f8ec37b420e095f3ba8323cc6"

        # Check if directory exists
        fullfilepath = os.path.join(os.sep,apogee_env(), "dr17/apogee/spectro/aspcap/dr17/synspec/")
        if not os.path.exists(fullfilepath):
            os.makedirs(fullfilepath)
        filename = "allVisit-dr17-synspec.fits"
        fullfilename = os.path.join(os.sep,fullfilepath, filename)
        url = f"https://data.sdss.org/sas/dr17/apogee/spectro/aspcap/dr17/synspec/{filename}"
    else:
        raise ValueError("allvisit() only supports APOGEE DR13-DR17")

    # check file integrity
    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash.lower():
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            allvisit(dr=dr, flag=1)
        else:
            logging.info(fullfilename + " was found!")
    elif not os.path.isfile(os.path.join(os.sep,fullfilepath, filename)) or flag == 1:
        with TqdmUpTo(
            unit="B", unit_scale=True, miniters=1, desc=url.split("/")[-1]
        ) as t:
            urllib.request.urlretrieve(url, fullfilename, reporthook=t.update_to)
            logging.info(
                f"Downloaded DR{dr:d} allVisit file catalog successfully to {fullfilepath}"
            )
            checksum = filehash(fullfilename, algorithm="sha1")
            if checksum != file_hash.lower():
                warnings.warn(
                    "File corruption detected, astroNN is attempting to download again"
                )
                allvisit(dr=dr, flag=1)

    return fullfilename


def combined_spectra(
    dr=None,
    location=None,
    field=None,
    apogee=None,
    telescope=None,
    verbose=1,
    flag=None,
):
    """
    Download the required combined spectra file a.k.a aspcapStar
    :param dr: APOGEE DR
    :type dr: int
    :param location: Location ID [Optional]
    :type location: int
    :param field: Field [Optional]
    :type field: str
    :param apogee: Apogee ID
    :type apogee: str
    :param telescope: Telescope ID, for example 'apo25m' or 'lco25m'
    :type telescope: str
    :param verbose: verbose, set 0 to silent most logging
    :type verbose: int
    :param flag: 0: normal, 1: force to re-download
    :type flag: int
    :return: full file path and download in background if not found locally, False if cannot be found on server
    :rtype: str
    :History:
        | 2017-Oct-15 - Written - Henry Leung (University of Toronto)
        | 2018-Aug-31 - Updated - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

    
    if dr == 17:
        reduce_prefix = "dr17"
        aspcap_code = "synspec"
        str1 = f"https://data.sdss.org/sas/dr17/apogee/spectro/aspcap/{reduce_prefix}/{aspcap_code}/{telescope}/{field}/"

        filename = f"aspcapStar-{reduce_prefix}-{apogee}.fits"
        hash_filename = f"{reduce_prefix}_{aspcap_code}_{telescope}_{field}.sha1sum"
        urlstr = str1 + filename

        # check folder existence
        fullfoldername = os.path.join(os.sep,
            apogee_env(),
            f"dr{dr}/apogee/spectro/aspcap/{reduce_prefix}/{aspcap_code}/{telescope}",
            str(f"{field}"),
        )
        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)

        fullfilename = os.path.join(os.sep,fullfoldername, filename)
    else:
        raise ValueError("combined_spectra() only supports APOGEE DR13-DR17")

    # check hash file
    full_hash_filename = os.path.join(os.sep,fullfoldername, hash_filename)
    if not os.path.isfile(full_hash_filename):
        # return warning flag if the location_id cannot even be found
        try:
            urllib.request.urlopen(str1)
        except urllib.error.HTTPError:
            return warning_flag
        urllib.request.urlretrieve(str1 + hash_filename, full_hash_filename)

    hash_list = np.loadtxt(full_hash_filename, dtype="str").T

    # In some rare case, the hash cant be found, so during checking, check len(file_has)!=0 too
    file_hash = hash_list[0][np.argwhere(hash_list[1] == filename)]

    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash and len(file_hash) != 0:
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            combined_spectra(
                dr=dr, location=location, apogee=apogee, verbose=verbose, flag=1
            )

        if verbose == 1:
            logging.info(fullfilename + " was found!")

    elif not os.path.isfile(fullfilename) or flag == 1:
        try:
            urllib.request.urlretrieve(urlstr, fullfilename)
            logging.info(
                f"Downloaded DR{dr} combined file successfully to {fullfilename}"
            )
            checksum = filehash(fullfilename, algorithm="sha1")
            if checksum != file_hash and len(file_hash) != 0:
                warnings.warn(
                    "File corruption detected, astroNN is attempting to download again"
                )
                combined_spectra(
                    dr=dr, location=location, apogee=apogee, verbose=verbose, flag=1
                )
        except urllib.error.HTTPError as emsg:
            if "401" in str(emsg):
                fullfilename = __apogee_credentials_downloader(urlstr, fullfilename)
            elif "404" in str(emsg):
                warnings.warn(f"{urlstr} cannot be found on server, skipped")
                fullfilename = warning_flag
            else:
                warnings.warn(f"Unknown error occurred - {emsg}")
                fullfilename = warning_flag

    return fullfilename


def visit_spectra(
    dr=None,
    location=None,
    field=None,
    apogee=None,
    telescope=None,
    verbose=1,
    flag=None,
    commission=False,
):
    """
    Download the required individual spectra file a.k.a apStar or asStar
    :param dr: APOGEE DR
    :type dr: int
    :param location: Location ID [Optional]
    :type location: int
    :param field: Field [Optional]
    :type field: str
    :param apogee: Apogee ID
    :type apogee: str
    :param telescope: Telescope ID, for example 'apo25m' or 'lco25m'
    :type telescope: str
    :param verbose: verbose, set 0 to silent most logging
    :type verbose: int
    :param flag: 0: normal, 1: force to re-download
    :type flag: int
    :param commission: whether the spectra is taken during commissioning
    :type commission: bool
    :return: full file path and download in background if not found locally, False if cannot be found on server
    :rtype: str
    :History:
        | 2017-Nov-11 - Written - Henry Leung (University of Toronto)
        | 2018-Aug-31 - Updated - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

   
    if dr == 17:
        reduce_prefix = "dr17"
        str1 = f"https://data.sdss.org/sas/dr17/apogee/spectro/redux/{reduce_prefix}/stars/{telescope}/{field}/"
        if telescope == "lco25m":
            if commission:
                filename = f"asStarC-{reduce_prefix}-{apogee}.fits"
            else:
                filename = f"asStar-{reduce_prefix}-{apogee}.fits"
        else:
            if commission:
                filename = f"apStarC-{reduce_prefix}-{apogee}.fits"
            else:
                filename = f"apStar-{reduce_prefix}-{apogee}.fits"
        urlstr = str1 + filename
        hash_filename = f"{reduce_prefix}_stars_{telescope}_{field}.sha1sum"

        fullfoldername = os.path.join(os.sep,
            apogee_env(),
            f"dr{dr}/apogee/spectro/redux/{reduce_prefix}/stars/{telescope}/",
            str(f"{field}"),
        )

        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)
    else:
        raise ValueError("visit_spectra() only supports APOGEE DR13-DR17")

    # check hash file
    full_hash_filename = os.path.join(os.sep,fullfoldername, hash_filename)
    if not os.path.isfile(full_hash_filename):
        # return warning flag if the location_id cannot even be found
        try:
            urllib.request.urlopen(str1)
        except urllib.error.HTTPError:
            return warning_flag
        urllib.request.urlretrieve(str1 + hash_filename, full_hash_filename)

    hash_list = np.loadtxt(full_hash_filename, dtype="str").T

    fullfilename = os.path.join(os.sep,fullfoldername, filename)

    # In some rare case, the hash cant be found, so during checking, check len(file_has)!=0 too
    # visit spectra has a different filename in checksum
    # handle the case where apogee_id cannot be found
    hash_idx = [
        i
        for i, item in enumerate(hash_list[1])
        if f"apStar-{reduce_prefix}-{apogee}" in item
    ]
    file_hash = hash_list[0][hash_idx]

    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash and len(file_hash) != 0:
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            visit_spectra(
                dr=dr, location=location, apogee=apogee, verbose=verbose, flag=1
            )

        if verbose:
            logging.info(fullfilename + " was found!")

    elif not os.path.isfile(fullfilename) or flag == 1:
        try:
            urllib.request.urlretrieve(urlstr, fullfilename)
            logging.info(
                f"Downloaded DR{dr} individual visit file successfully to {fullfilename}"
            )
            checksum = filehash(fullfilename, algorithm="sha1")
            if checksum != file_hash and len(file_hash) != 0:
                warnings.warn(
                    "File corruption detected, astroNN is attempting to download again"
                )
                visit_spectra(
                    dr=dr, location=location, apogee=apogee, verbose=verbose, flag=1
                )
        except urllib.error.HTTPError as emsg:
            if "401" in str(emsg):
                fullfilename = __apogee_credentials_downloader(urlstr, fullfilename)
            elif "404" in str(emsg):
                warnings.warn(f"{urlstr} cannot be found on server, skipped")
                fullfilename = warning_flag
            else:
                warnings.warn(f"Unknown error occurred - {emsg}")
                fullfilename = warning_flag

    return fullfilename


def apogee_rc(dr=None, flag=None):
    """
    Download the APOGEE red clumps catalogue
    :param dr: Apogee DR
    :type dr: int
    :param flag: Force to download if flag=1
    :type flag: int
    :return: full file path
    :rtype: str
    :History: 2017-Nov-16 - Written - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

    if dr == 17:
        file_hash = "d54e0ea4e6a3f5cc3c02a73b93260e992d9836d0"

        str1 = "https://data.sdss.org/sas/dr17/apogee/vac/apogee-rc/cat/"
        filename = f"apogee-rc-DR{dr}.fits"
        urlstr = str1 + filename
        fullfoldername = os.path.join(os.sep,apogee_env(), "dr17/apogee/vac/apogee-rc/cat/")
        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)
        fullfilename = os.path.join(os.sep,fullfoldername, filename)

    else:
        raise ValueError("apogee_rc() only supports APOGEE DR13-DR17")

    # check file integrity
    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash.lower():
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            apogee_rc(dr=dr, flag=1)
        else:
            logging.info(fullfilename + " was found!")

    elif not os.path.isfile(fullfilename) or flag == 1:
        try:
            with TqdmUpTo(
                unit="B", unit_scale=True, miniters=1, desc=urlstr.split("/")[-1]
            ) as t:
                urllib.request.urlretrieve(urlstr, fullfilename, reporthook=t.update_to)
                logging.info(
                    f"Downloaded DR{dr} Red Clumps Catalog successfully to {fullfilename}"
                )
                checksum = filehash(fullfilename, algorithm="sha1")
                if checksum != file_hash.lower():
                    warnings.warn(
                        "File corruption detected, astroNN is attempting to download again"
                    )
                    apogee_rc(dr=dr, flag=1)
        except urllib.error.HTTPError as emsg:
            if "401" in str(emsg):
                fullfilename = __apogee_credentials_downloader(urlstr, fullfilename)
            elif "404" in str(emsg):
                warnings.warn(f"{urlstr} cannot be found on server, skipped")
                fullfilename = warning_flag
            else:
                warnings.warn(f"Unknown error occurred - {emsg}")
                fullfilename = warning_flag

    return fullfilename


def apogee_distances(dr=None, flag=None):
    """
    Download the APOGEE Distances VAC catalogue (APOGEE Distances for DR14, APOGEE Starhourse for DR16/17)
    :param dr: APOGEE DR
    :type dr: int
    :param flag: Force to download if flag=1
    :type flag: int
    :return: full file path
    :rtype: str
    :History:
        | 2018-Jan-24 - Written - Henry Leung (University of Toronto)
        | 2021-Jan-29 - Updated - Henry Leung (University of Toronto)
    """
    dr = apogee_default_dr(dr=dr)

    if dr == 17:
        file_hash = "2502e2f7703046163f81ecc4054dce39b2038e4f"

        str1 = "https://data.sdss.org/sas/dr17/apogee/vac/apogee-starhorse/"
        filename = f"APOGEE_DR17_EDR3_STARHORSE_v2.fits"
        urlstr = str1 + filename
        fullfoldername = os.path.join(os.sep,apogee_env(), "dr17/apogee/vac/apogee-starhorse/")
        if not os.path.exists(fullfoldername):
            os.makedirs(fullfoldername)
        fullfilename = os.path.join(os.sep,fullfoldername, filename)
    else:
        raise ValueError("apogee_distances() only supports APOGEE DR14-DR17")

    # check file integrity
    if os.path.isfile(fullfilename) and flag is None:
        checksum = filehash(fullfilename, algorithm="sha1")
        if checksum != file_hash.lower():
            warnings.warn(
                "File corruption detected, astroNN is attempting to download again"
            )
            apogee_distances(dr=dr, flag=1)
        else:
            logging.info(fullfilename + " was found!")

    elif not os.path.isfile(fullfilename) or flag == 1:
        try:
            with TqdmUpTo(
                unit="B", unit_scale=True, miniters=1, desc=urlstr.split("/")[-1]
            ) as t:
                urllib.request.urlretrieve(urlstr, fullfilename, reporthook=t.update_to)
                logging.info(
                    f"Downloaded DR{dr} Distances successfully to {fullfilename}"
                )
                checksum = filehash(fullfilename, algorithm="sha1")
                if checksum != file_hash.lower():
                    warnings.warn(
                        "File corruption detected, astroNN is attempting to download again"
                    )
                    apogee_distances(dr=dr, flag=1)
        except urllib.error.HTTPError:
            warnings.warn(f"{urlstr} cannot be found on server, skipped")
            fullfilename = warning_flag

    return fullfilename