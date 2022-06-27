# import required libraries
import random
import pytest
import numpy as np
from datetime import datetime
from synthetic_data_ingestion.sample_creator import SynthCustomers


# define samples to parameterize TestSynthCustomers class
samples = [(10, "CONTROL"), (100, "TREATMENT"), (1000, "CONTROL")]

# parameterize class
@pytest.mark.parametrize("num_samples,group", samples)
class TestSynthCustomers:
    def test_constructor_group(self, num_samples, group):
        """test correct construction of object on random initialization.
        It must have the right group attribute, the right num_samples atttribute
        and a sampling_dict attribute (empty dict)"""

        random_num_samples = random.randint(1, 9)
        random_group = random.choice(["CONTROL", "TREATMENT"])

        test_object = SynthCustomers(num_samples=random_num_samples, group=random_group)

        assert (
            (test_object.group == random_group)
            and (test_object.num_samples == random_num_samples)
            and ("sampling_dict" in test_object.__dict__.keys())
        )

    def test_constructor_group_type(self, num_samples, group):
        """group param must be a string"""
        with pytest.raises(TypeError):
            SynthCustomers(num_samples=10, group=10)

    def test_constructor_group_value(self, num_samples, group):
        '''group param must be "CONTROL" or "TREATMENT"'''
        with pytest.raises(ValueError):
            SynthCustomers(num_samples=10, group="WRONG")

    def test_constructor_num_samples_type(self, num_samples, group):
        """num_samples param must be an integer"""
        with pytest.raises(TypeError):
            SynthCustomers(num_samples=7.5, group="CONTROL")

    def test_constructor_num_samples_value(self, num_samples, group):
        """num_samples param must be an integer >= 1"""
        with pytest.raises(ValueError):
            SynthCustomers(num_samples=0, group="CONTROL")

    def test_gen_group_type(self, num_samples, group):
        """group attribute of random sampling must be of type string ("<U7" or "<09")"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_group()

        assert synth_customers.sampling_dict["group"].dtype in ("<U7", "<U9")

    def test_gen_group_values(self, num_samples, group):
        '''group attribute of random sampling must be "CONTROL" and/or "TREATMENT"'''

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_group()

        assert set(synth_customers.sampling_dict["group"]) <= set(
            synth_customers.groups
        )

    def test_gen_total_purchase_price_type(self, num_samples, group):
        """total_purchase_price attribute type must be float16"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_total_purchase_price()

        assert synth_customers.sampling_dict["total_purchase_price"].dtype == "float16"

    def test_gen_total_purchase_price_min(self, num_samples, group):
        """minimum value of total_purchase_price attribute of random sampling must > 0"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_total_purchase_price()

        assert np.min(synth_customers.sampling_dict["total_purchase_price"]) > 0

    def test_gen_num_diff_items_type(self, num_samples, group):
        """num_diff_items attribute of random sampling must be of type int16"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_num_diff_items()

        assert synth_customers.sampling_dict["num_diff_items"].dtype == "int16"

    def test_gen_num_diff_items_min(self, num_samples, group):
        """minimum value of num_diff_items attribute of random sampling must > 0"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_num_diff_items()

        assert np.min(synth_customers.sampling_dict["num_diff_items"]) > 0

    def test_gen_purchase_date_type(self, num_samples, group):
        """purchase_date attribute of random sampling must be a string (numpy <U10)"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_purchase_date()

        assert synth_customers.sampling_dict["purchase_date"].dtype == "<U10"

    def test_gen_purchase_date_values(self, num_samples, group):
        """the difference between the latest and the earliest purchase_date
        must not be greater than 6 days"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_purchase_date()

        date_list = [
            datetime.strptime(str_date, "%Y-%m-%d").date()
            for str_date in set(synth_customers.sampling_dict["purchase_date"])
        ]

        min_date = min(date_list)
        max_date = max(date_list)

        assert (max_date - min_date).days <= 6

    def test_gen_region_type(self, num_samples, group):
        """region attribute of random sampling must be a string (numpy <U3)"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_region()

        assert synth_customers.sampling_dict["region"].dtype == "<U3"

    def test_gen_region_values(self, num_samples, group):
        """region attribute of random sampling must be within available regions"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_region()

        assert set(synth_customers.sampling_dict["region"]) <= set(
            synth_customers.region
        )

    def test_gen_gender_type(self, num_samples, group):
        """gender attribute of random sampling must be a string (numpy "<U6")"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_gender()

        assert synth_customers.sampling_dict["gender"].dtype == "<U6"

    def test_gen_gender_values(self, num_samples, group):
        """gender attribute of random sampling must be within available gender"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_gender()

        assert set(synth_customers.sampling_dict["gender"]) <= set(
            synth_customers.gender
        )

    def test_gen_device_type(self, num_samples, group):
        """device attribute of random sampling must be a string (numpy "<U8")"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_device()

        assert synth_customers.sampling_dict["device"].dtype == "<U8"

    def test_gen_device_values(self, num_samples, group):
        """device attribute of random sampling must be within available devices"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.gen_device()

        assert set(synth_customers.sampling_dict["device"]) <= set(
            synth_customers.device
        )

    def test_generate_samples_variables(self, num_samples, group):
        """check if random generated data for users has the expected
        variables to be recorded for every user"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()

        expected_user_variables = {
            "total_purchase_price",
            "num_diff_items",
            "purchase_date",
            "region",
            "gender",
            "group",
            "device",
        }

        assert set(synth_customers.sampling_dict.keys()) == expected_user_variables

    def test_generate_samples_num_users(self, num_samples, group):
        """check if random generated data for users has the expected number of users"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()

        number_of_users = [
            len(array) for array in synth_customers.sampling_dict.values()
        ]

        assert set(number_of_users) == {synth_customers.num_samples}

    def test_generate_report_variables(self, num_samples, group):
        """check if report of random generated data for users has
        the expected variables"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)
        synth_customers.generate_samples()
        synth_customers.generate_report()

        expected_report_variables = {
            "group",
            "gamma_shape",
            "gamma_scale",
            "poisson_lambda",
            "date_interval",
            "region",
            "gender",
            "device",
        }

        assert set(synth_customers.creation_report.keys()) == expected_report_variables

    def test_generate_report_sampling_not_created(self, num_samples, group):
        """check if generate_report raises error
        in case of sampling was not created previously"""

        synth_customers = SynthCustomers(num_samples=num_samples, group=group)

        with pytest.raises(Exception):
            synth_customers.generate_report()
