import string
import random
import time
import pandas as pd


RANDOM_SEED = 42
<<<<<<< HEAD
N_USERS = 1000000
=======
N_USERS = 10000
>>>>>>> 52e37758ffdee5281e60e9ad60b6a44c7e12e659
MAX_CLICKS_PER_USER = 40
MAX_PHOTOS_PER_USER = 20
ORDER_CHANCE_PER_CLICK = 0.2
STRING_CHARS = string.ascii_letters + string.digits

countries = ["US", "UK", "DE", "CN", "JP", "FR", "PT", "IT", "IN", "RO", "CA"]
def random_string(str_size, allowed_chars=string.ascii_letters):
    return ''.join(random.choice(allowed_chars) for x in range(str_size))


def create_users(n_users, id_length=8, reg_date_length=8, country_length=2):
    user_list = []
    for _ in range(n_users):
        user = [
            int(random.random() * 10 ** id_length),
            #random_string(reg_date_length, STRING_CHARS),
            int(random.random() * 10 ** reg_date_length),
            random.choice(countries)
        ]
        user_list.append(user)
    return pd.DataFrame(user_list, columns=["id", "reg_date", "country"])


def create_photos(users, max_photos_per_user, id_length=8, title_length=20, max_views=1000, details_length=20, date_length=8):
    photo_list = []
    for idx, row in users.iterrows():
        for _ in range(int(random.random() * max_photos_per_user)):
            photo = [
                int(random.random() * 10 ** id_length),
                random_string(title_length, STRING_CHARS),
                row["id"],
                int(random.random() * max_views),
                random_string(details_length, STRING_CHARS),
                int(random.random() * 10 ** date_length)
            ]
            photo_list.append(photo)
    return pd.DataFrame(photo_list, columns=["id", "title", "userId", "views", "details", "date"])


def create_clicks(users, max_clicks_per_user, id_length=8, url_length=30, details_length=50):
    click_list = []
    for idx, row in users.iterrows():
        for _ in range(int(random.random() * max_clicks_per_user)):
            click = [
                int(random.random() * 10 ** id_length),
                row["id"],
                random_string(url_length, STRING_CHARS),
                random_string(details_length, STRING_CHARS)
            ]
            click_list.append(click)
    return pd.DataFrame(click_list, columns=["id", "userId", "url", "details"])


def create_orders(clicks, photos, order_chance_per_click, id_length=8, n_types=5, max_price=100):
    order_list = []
    for idx, row in clicks.iterrows():
        if random.random() < order_chance_per_click:
            order = [
                int(random.random() * 10 ** id_length),
                int(random.random() * n_types),
                row["url"],
                photos["id"].sample().iloc[0],
                random.random() * max_price,
                random.choice(countries)
            ]
            order_list.append(order)
    return pd.DataFrame(order_list, columns=["id", "type", "source", "photoId", "price","country"])


if __name__ == '__main__':
    start_time = time.time()
    checkpoint_time = time.time()
    random.seed(RANDOM_SEED)

    users = create_users(N_USERS)
    print(f"--- Users creation time: {time.time() - checkpoint_time:.5f} seconds ---")
    checkpoint_time = time.time()

    photos = create_photos(users, MAX_PHOTOS_PER_USER)
    print(f"--- Photos creation time: {time.time() - checkpoint_time:.5f} seconds ---")
    checkpoint_time = time.time()

    clicks = create_clicks(users, MAX_CLICKS_PER_USER)
    print(f"--- Clicks creation time: {time.time() - checkpoint_time:.5f} seconds ---")
    checkpoint_time = time.time()

    orders = create_orders(clicks, photos, ORDER_CHANCE_PER_CLICK)
    print(f"--- Orders creation time: {time.time() - checkpoint_time:.5f} seconds ---")
    checkpoint_time = time.time()

<<<<<<< HEAD
    users.to_csv("../sample_data/big/users.csv", index=False, header=False)
    photos.to_csv("../sample_data/big/photos.csv", index=False, header=False)
    clicks.to_csv("../sample_data/big/clicks.csv", index=False, header=False)
    orders.to_csv("../sample_data/big/orders.csv", index=False, header=False)
=======
    users.to_csv("../sample_data/users.csv", index=False, header=False)
    photos.to_csv("../sample_data/photos.csv", index=False, header=False)
    clicks.to_csv("../sample_data/clicks.csv", index=False, header=False)
    orders.to_csv("../sample_data/orders.csv", index=False, header=False)
>>>>>>> 52e37758ffdee5281e60e9ad60b6a44c7e12e659

    print(f"--- Total time: {time.time() - start_time:.5f} seconds ---")
