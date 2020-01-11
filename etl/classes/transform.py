from typing import List


class Transform:
    def __init__(self, leads_df, reviews_df):
        self.leads_df = leads_df
        self.reviews_df = reviews_df

    def remove_duplicated_leads(self, columns: List):
        num_rows = self.leads_df.shape[0]
        self.leads_df = Transform.__remove_duplicates__(self.leads_df, columns=columns)

        return num_rows - self.leads_df.shape[0]

    def remove_duplicated_reviews(self, columns: List):
        num_rows = self.reviews_df.shape[0]

        self.reviews_df = Transform.__remove_duplicates__(self.reviews_df, columns=columns)

        return num_rows - self.reviews_df.shape[0]

    @staticmethod
    def __remove_duplicates__(df, columns: List):
        if df.duplicated(columns).sum() > 0:
            df.drop_duplicates(columns, inplace=True, keep='last')

        return df


