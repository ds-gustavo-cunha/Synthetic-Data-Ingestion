# import required libraries
import sys
import time
import logging
import numpy as np
from datetime import datetime, timedelta


class SynthGenBase:
    """Base class to create sythetic customer behaviour"""

    def __init__(self):
        """DEFINE CONSTANTS"""

        ####### TOTAL PURCHASE PRICE
        # instanciate shape and scale values for gamma distribution
        self.gamma_shape = [1, 2, 3, 5, 7]
        self.gamma_scale = [10]

        ####### NUMBER OF DIFFERENT ITEMS #######
        # instanciate possible lambda values for poisson distribution
        self.poisson_lambda = [1, 2, 3, 4, 5]

        ####### REGION #######
        # LAM = Latin America, NAM = North America, EUR = Europe, AFR = Africa, ASA = Asia
        # instanciate possible customer regions
        self.region = ["LAM", "NAM", "EUR", "AFR", "ASA"]
        # instanciate region weights -> reminder to check different values in future
        # initially, uniform distribution
        self.region_weights = [1 / len(self.region)] * len(self.region)

        ####### GENDER #######
        # instanciate possible customer genders
        self.gender = ["MALE", "FEMALE"]
        # instanciate gender weights -> reminder to check different values in future
        # initially, uniform distribution
        self.gender_weights = [1 / len(self.gender)] * len(self.gender)

        ####### DEVICE #######
        # instanciate possible devices
        self.device = ["MOBILE", "COMPUTER"]
        # instanciate device weights -> reminder to check different values in future
        # initially, uniform distribution
        self.device_weights = [1 / len(self.device)] * len(self.device)

        ####### GROUPS #######
        # instanciate possible groups
        self.groups = ["CONTROL", "TREATMENT"]

    def input_validation(self, group: str, num_samples: int) -> None:
        """Validate user input in regard to group names and number of samples params"""

        # validate user input -> group = str
        if not isinstance(group, str):
            # raise value error with problem indication
            raise TypeError("group param must be a string")

        # validate user input -> group = "CONTROL" or "TREATMENT"
        if not group in ("CONTROL", "TREATMENT"):
            # raise value error with problem indication
            raise ValueError('group param must be "CONTROL" or "TREATMENT"')

        # validate user input -> num_samples = integer
        if not isinstance(num_samples, int):
            # raise value error with problem indication
            raise TypeError("num_samples param must be an integer")

        # validate user input -> num_samples >= 1
        if not num_samples >= 1:
            # raise value error with problem indication
            raise ValueError("num_samples param must be an integer >= 1")

        return None  # explicitly


class SynthCustomers(SynthGenBase):
    """Class to generate synthetic customer behavior given user inputs"""

    def __init__(self, num_samples: int, group: str, log_folder: str = None):
        """Object constructor

        Args
            num_samples: an integer with the number of synthetic customers to generates
            group: a string ("CONTROL" or "TREATMENT") to indicate AB-testing group
            log_folder: a string with the path to store logs"""


        # inherit from father class
        super().__init__()

        # instanciate logger
        self.logger = logging.getLogger("sample_creator.py")

        # define log date in utc
        logging.Formatter.converter = time.gmtime

        # check if user input a folder to store logs
        if log_folder is None:
            # set a default folder
            log_folder = "../logs"

        # define logging configuration
        logging.basicConfig(
            filename=f"{log_folder}/data_ingestion-{datetime.utcnow().date()}.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y:%m:%d %H:%M:%S",
        )

        # try to validate input
        try:
            # validate user inputs before assigning them to object attributes
            self.input_validation(group=group, num_samples=num_samples)

        # input not valid
        except Exception as e:
            # log a warning
            self.logger.warning(
                f"SynthCustomers object NOT instanciated: raised error ---> {e}"
            )

            # raise the exception
            raise e

        # input validated
        else:
            # define group attribute
            self.group = group
            # define num_samples attribute
            self.num_samples = num_samples

            # define dictionary that will hold synthetic data
            self.sampling_dict = {}

            # flag to indicate if sampling was created
            self.sampling_created = False

            # log an information
            self.logger.info(
                f"SynthCustomers object successfully instanciated: group = {self.group}, num_samples = {self.num_samples}"
            )

    def gen_group(self) -> None:
        """Generate group attribute for the given object"""

        # group label to be used on A/B testing
        self.sampling_dict["group"] = np.array([self.group] * self.num_samples)

        # log a debug
        self.logger.debug(f"gen_group method successfully called: group = {self.group}")

        return None  # explicitly

    def gen_total_purchase_price(self) -> None:
        """Generate group total_purchase_price attribute for the given object using
        a gamma distribution so as to have purchase as a continuous right skewed distribution"""

        # define numpy number generator
        np_gen = np.random.default_rng()
        # chose a random shape a scale params to create the gamma distribution
        self.random_shape = np.random.choice(self.gamma_shape, size=1)
        self.random_scale = np.random.choice(self.gamma_scale, size=1)

        # generate gamma distribution
        self.sampling_dict["total_purchase_price"] = np.float16(
            np_gen.gamma(
                shape=self.random_shape, scale=self.random_scale, size=self.num_samples
            )
        )

        # log a debug
        self.logger.debug(
            f"gen_total_purchase_price method successfully called: shape = {self.random_shape[0]}, scale = {self.random_scale[0]}"
        )

        return None  # explicitly

    def gen_num_diff_items(self) -> None:
        """Generate group num_diff_items attribute for the given object using
        a poisson distribution so as to have purchase as a discrete right skewed distribution"""

        # define numpy number generator
        np_gen = np.random.default_rng()

        # choose a random poisson lambda to create the poisson distribution
        self.random_lam = np.random.choice(self.poisson_lambda, size=1)

        # generate poisson distribution
        # the "+1" is to avoid getting number of different items equal to zero
        # once poisson distribution start from 0
        self.sampling_dict["num_diff_items"] = np.int16(
            np_gen.poisson(lam=self.random_lam, size=self.num_samples) + 1
        )

        # log a debug
        self.logger.debug(
            f"gen_num_diff_items method successfully called: lam = {self.random_lam[0]}"
        )

        return None  # explicitly

    def gen_purchase_date(self) -> None:
        """Generate purchase_date attribute for the given object using
        the ingestion date (in UTC) as a refence and randomly assign number of days
        so as to cover one week interval (from "ingestion date" to "ingestion date + 6 days")"""

        # define generating date as an attribute
        self.gen_date_utc = datetime.utcnow()

        # generate synthetic random dates from "ingestion date" to "ingestion date + 6 days"
        self.sampling_dict["purchase_date"] = np.array(
            [
                str(
                    # present date in UTC
                    self.gen_date_utc.date()
                    +
                    # random number of days
                    timedelta(days=np.random.randint(low=0, high=7))
                )
                # iterate "num_samples" times
                for _ in range(self.num_samples)
            ]
        )

        # log a debug
        self.logger.debug(
            f"gen_purchase_date method successfully called: reference date (in UTC) = {self.gen_date_utc.date()}"
        )

        return None  # explicitly

    def gen_region(self) -> None:
        """Generate region attribute for the given object with synthetic data"""
        # Region meaning:
        # LAM = Latin America, NAM = North America, EUR = Europe, AFR = Africa, ASA = Asia

        # generate synthetic region data
        self.sampling_dict["region"] = np.random.choice(
            self.region,
            size=self.num_samples,  # number of samples
            replace=True,  # create variability
            p=self.region_weights,  # weight of regions
        )

        # log a debug
        self.logger.debug(
            f"gen_region method successfully called: region weights [LAM-NAM-EUR-AFR-ASA] = {self.region_weights}"
        )

        return None  # explicitly

    def gen_gender(self) -> None:
        """Generate gender attribute for the given object with synthetic data"""

        # generate synthetic customer gender data
        self.sampling_dict["gender"] = np.random.choice(
            self.gender,
            size=self.num_samples,  # number of samples
            replace=True,  # create variability
            p=self.gender_weights,  # gender weights
        )

        # log a debug
        self.logger.debug(
            f"gen_gender method successfully called: gender weights [MALE-FEMALE] = {self.region_weights}"
        )

        return None  # explicitly

    def gen_device(self) -> None:
        """Generate device attribute for the given object with synthetic data"""

        # generate synthetic customer device data
        self.sampling_dict["device"] = np.random.choice(
            self.device,
            size=self.num_samples,  # number of samples
            replace=True,  # create variability
            p=self.device_weights,  # device weights
        )

        # log a debug
        self.logger.debug(
            f"gen_device method successfully called: device weights [MOBILE-COMPUTER] = {self.device_weights}"
        )

        return None  # explicitly

    def generate_samples(self):
        """Main function that create all the needed object attributes given the available methods"""

        # generate total_purchase_price data
        self.gen_total_purchase_price()
        # generate num_diff_items data
        self.gen_num_diff_items()
        # generate purchase_date data
        self.gen_purchase_date()
        # generate region data
        self.gen_region()
        # generate gender data
        self.gen_gender()
        # generate group data
        self.gen_group()
        # generate device data
        self.gen_device()

        # change sampling created flag
        self.sampling_created = True

        # log an information
        self.logger.info(f"generate_samples method successfully called.")

        return None  # explicitly

    def generate_report(self) -> dict:
        """Record the params used to generate the synthetic data for the given object"""

        # check if sampling was created before report
        if self.sampling_created:

            # create report attribute that will hold creation variables in a dict format
            self.creation_report = {
                "group": self.group,
                "gamma_shape": self.random_shape[0],
                "gamma_scale": self.random_scale[0],
                "poisson_lambda": self.random_lam[0],
                "date_interval": f"[{self.gen_date_utc.date()},{self.gen_date_utc.date() + timedelta(days = 6)}] [extremes included]",
                "region": dict(zip(self.region, self.region_weights)),
                "gender": dict(zip(self.gender, self.gender_weights)),
                "device": dict(zip(self.device, self.device_weights)),
            }

            # log an information
            self.logger.info(f"generate_report method successfully called.")

            return self.creation_report

        # sampling was not created before report
        else:

            # log a warning
            self.logger.warning(
                f"generate_report method called. Raised error ---> generate_samples method must be called before random_creation_report method"
            )

            raise Exception(
                "You need to create sampling (via generate_samples method) before making the report (via random_creation_report method)"
            )
