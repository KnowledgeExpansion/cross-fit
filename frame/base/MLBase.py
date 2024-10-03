import numpy as np

from frame.base.CompBase import *


RegressionType = enum('GPR', 'NON_GPR')


class MLBase(CompBase):
    def __init__(self, title='', module_name='MLBase', log_level=2):
        CompBase.__init__(self, module_name=module_name, log_level=log_level)
        self.ml_model = None
        self.train = None
        self.test = None
        self.ml_name = None

    def set_ml_name(self):
        return self.ml_name

    def get_ml_name(self):
        return self.ml_name

    def set_train(self, data):
        self.train = data

    def get_train(self):
        return self.train

    def set_test(self, data):
        self.test = data

    def get_test(self):
        return self.test

    def clear_model(self):
        self.ml_model = None

    def get_ml_model(self):
        return self.ml_model

    def evaluation(self, new_data):
        self.ml_model.predict()

    def preprocess(self):
        pass

    def fit(self, input=None, output=None, **kwargs):
        self.ml_model.fit()


