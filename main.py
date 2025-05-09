from __future__ import annotations

import logging
import os
from flask import Flask, request
import sqlalchemy
from connect_connector import connect_with_connector

BUSINESSES = 'businesses'
REVIEWS = 'reviews'
USERS = 'users'
OWNERS = 'owners'
ERROR_MISSING_ATTRIBUTES = {"Error": "The request body is missing at least one of the required attributes"}
ERROR_BUSINESS_NOT_FOUND = {"Error": "No business with this business_id exists"}
ERROR_REVIEW_CONFLICT = {"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}
ERROR_REVIEW_NOT_FOUND = {"Error": "No review with this review_id exists"}


app = Flask(__name__)
logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
    raise ValueError('Missing database connection type. Please define INSTANCE_CONNECTION_NAME')

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

# create 'Businesses' and 'reviews' tables in database if it does not already exist
def create_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS businesses '
                '(id SERIAL NOT NULL, '
                'owner_id INT NOT NULL, '
                'name VARCHAR(50) NOT NULL, '
                'street_address VARCHAR(100) NOT NULL, '
                'city VARCHAR(50) NOT NULL, '
                'state VARCHAR(2) NOT NULL, '
                'zip_code VARCHAR(5) NOT NULL, '
                'PRIMARY KEY (id) );'
            )
        )

        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS reviews '
                '(id SERIAL NOT NULL, '
                'user_id INT NOT NULL, '
                'business_id INT NOT NULL, '
                'stars INT NOT NULL CHECK (stars >= 0 AND stars <= 5), '
                'review_text VARCHAR(1000), '
                'PRIMARY KEY (id) );'
            )
        )
        conn.commit()


# Create a Business
@app.route('/' + BUSINESSES, methods=['POST'])
def post_business():
    content = request.get_json()
    if len(content) != 6:
        return ERROR_MISSING_ATTRIBUTES, 400
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
    with db.connect() as conn:
        # Preparing a statement before hand can help protect against injections.
        stmt = sqlalchemy.text(
            'INSERT INTO businesses(owner_id, name, street_address, city, state, zip_code) '
            ' VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
        )
        # connection.execute() automatically starts a transaction
        conn.execute(stmt, parameters={'owner_id': content['owner_id'],
                                        'name': content['name'],
                                        'street_address': content['street_address'],
                                        'city': content['city'],
                                        'state': content['state'],
                                        'zip_code': content['zip_code']})
        # The function last_insert_id() returns the most recent value
        # generated for an `AUTO_INCREMENT` column when the INSERT
        # statement is executed
        stmt = sqlalchemy.text('SELECT last_insert_id()')
        # scalar() returns the first column of the first row or None if there are no rows
        b_id = conn.execute(stmt).scalar()
        # Remember to commit the transaction
        conn.commit()
        b_url = str(request.url) + str(b_id)
    
        return ({'id': b_id,
            'owner_id': content['owner_id'],
            'name': content['name'],
            'street_address': content['street_address'],
            'city': content['city'],
            'state': content['state'],
            'zip_code': content['zip_code'],
            'self': b_url}, 201)



# Get a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['GET'])
def get_business(business_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM businesses WHERE id=:business_id;'
        )
        # one_or_none returns at most one result or raise an exception.
        # returns None if the result has no rows.
        row = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
        if row is None:
            return ERROR_BUSINESS_NOT_FOUND, 404
        else:
            business = row._asdict()
            business['self'] = str(request.base_url)
            return business
    


# Get all businesses or add params for limit and offset
@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    offset = request.args.get('offset', default=0, type=int)
    limit = request.args.get('limit', default=3, type=int)

    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM businesses ORDER BY id LIMIT :limit OFFSET :offset'
        )
        rows = conn.execute(stmt, {'limit': limit, 'offset': offset}).mappings().all()

        businesses = []
        for row in rows:
            row_dict = dict(row)
            row_dict['self'] = str(request.url_root) + BUSINESSES + '/' + str(row_dict['id'])
            businesses.append(row_dict)

        next_offset = offset + limit
        next_url = f"{request.url_root}{BUSINESSES}?offset={next_offset}&limit={limit}"

        return {
            'entries': businesses,
            'next': next_url
        }, 200



# Get all businesses for an owner
@app.route('/' + OWNERS + '/<int:owner_id>/' + BUSINESSES, methods=['GET'])
def get_businesses__of_owner(owner_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM businesses WHERE owner_id=:owner_id'
        )
        rows = conn.execute(stmt, parameters={'owner_id': owner_id}).mappings().all()
        businesses = []
        for row in rows:
            row_dict = dict(row)
            b_id = row_dict['id']
            b_url = str(request.url_root) + BUSINESSES + '/' + str(b_id)
            row_dict['self'] = b_url
            businesses.append(row_dict)
        return businesses



# Update a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['PUT'])
def put_business(business_id):
    content = request.get_json()
    if len(content) != 6:
        return ERROR_MISSING_ATTRIBUTES, 400
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM businesses WHERE id=:business_id'
        )
        row = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
        if row is None:
            return ERROR_BUSINESS_NOT_FOUND, 404
        else:
            stmt = sqlalchemy.text(
                'UPDATE businesses '
                'SET name = :name, owner_id = :owner_id, street_address = :street_address, city = :city, state = :state, zip_code = :zip_code '
                'WHERE id = :business_id'
            )
            conn.execute(stmt, parameters={
                'name': content['name'],
                'owner_id': content['owner_id'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'business_id': business_id
            })
            conn.commit()
            return {
                'id': business_id,
                'name': content['name'],
                'owner_id': content['owner_id'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code'],
                'self': str(request.url)
            }



# Delete a business
@app.route('/' + BUSINESSES + '/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    with db.connect() as conn:
        stmt1 = sqlalchemy.text(
            'DELETE FROM reviews WHERE business_id=:business_id'
        )
        row1 = conn.execute(stmt1, parameters={'business_id': business_id})


        stmt2 = sqlalchemy.text(
            'DELETE FROM businesses WHERE id=:business_id'
        )
        result = conn.execute(stmt2, parameters={'business_id': business_id})
        conn.commit()
        # result.rowcount value will be the number of rows deleted.
        # For our statement, the value be 0 or 1 because lodging_id is
        # the PRIMARY KEY
        if result.rowcount == 1:
            return ('', 204)
        else:
            return ERROR_BUSINESS_NOT_FOUND, 404



# Create a Review
@app.route('/' + REVIEWS, methods=['POST'])
def post_review():
    content = request.get_json()
    if content.get('user_id')is None or content.get('business_id') is None or content.get('stars') is None:
        return ERROR_MISSING_ATTRIBUTES, 400
    
    with db.connect() as conn:
        stmt = sqlalchemy.text('SELECT id FROM businesses WHERE id = :business_id')
        row = conn.execute(stmt, {'business_id': content['business_id']}).one_or_none()
        if row is None:
            return ERROR_BUSINESS_NOT_FOUND, 404

        stmt = sqlalchemy.text(
            'SELECT business_id, user_id FROM reviews WHERE business_id = :business_id AND user_id = :user_id'
        )
        row = conn.execute(stmt, {
            'business_id': content['business_id'],
            'user_id': content['user_id']
        }).one_or_none()
        if row is not None:
            return ERROR_REVIEW_CONFLICT, 409

        stmt = sqlalchemy.text(
            'INSERT INTO reviews(user_id, business_id, stars, review_text) '
            'VALUES (:user_id, :business_id, :stars, :review_text)'
        )
        conn.execute(stmt, {
            'user_id': content['user_id'],
            'business_id': content['business_id'],
            'stars': content['stars'],
            'review_text': content.get('review_text', '')
        })

        stmt2 = sqlalchemy.text('SELECT last_insert_id()')
        r_id = conn.execute(stmt2).scalar()

        conn.commit()

        r_url = str(request.url) + '/' + str(r_id)
        b_url = str(request.url_root) + BUSINESSES + '/' + str(content['business_id'])

        return ({
            'id': r_id,
            'user_id': content['user_id'],
            'business': b_url,
            'stars': content['stars'],
            'review_text': content.get('review_text', ''),
            'self': r_url
        }, 201)



# Get a review
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['GET'])
def get_review(review_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM reviews WHERE id=:review_id;'
        )
        # one_or_none returns at most one result or raise an exception.
        # returns None if the result has no rows.
        row = conn.execute(stmt, parameters={'review_id': review_id}).one_or_none()
        if row is None:
            return ERROR_REVIEW_NOT_FOUND, 404
        else:
            review = row._asdict()
            b_id = review['business_id']
            b_url = str(request.url_root) + 'businesses/' + str(b_id)
            r_url = str(request.url)
            return {
                'id': review['id'],
                'user_id': review['user_id'],
                'business': b_url,
                'stars': review['stars'],
                'review_text': review.get('review_text', ''),
                'self': r_url
            }
  


# Update a review
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['PUT'])
def put_review(review_id):
    content = request.get_json()
    if content.get('stars') is None:
        return ERROR_MISSING_ATTRIBUTES, 400

    with db.connect() as conn:
        stmt = sqlalchemy.text('SELECT * FROM reviews WHERE id = :review_id')
        row = conn.execute(stmt, {'review_id': review_id}).one_or_none()
        if row is None:
            return ERROR_REVIEW_NOT_FOUND, 404

        row_dict = row._asdict()
        b_id = row_dict['business_id']
        b_url = str(request.url_root) + BUSINESSES + '/' + str(b_id)
        r_url = str(request.url)

        review_text = content.get('review_text', row_dict['review_text'])

        stmt = sqlalchemy.text(
            'UPDATE reviews '
            'SET stars = :stars, review_text = :review_text '
            'WHERE id = :review_id'
        )
        conn.execute(stmt, {
            'stars': content['stars'],
            'review_text': review_text,
            'review_id': review_id
        })
        conn.commit()

        return {
            'id': row_dict['id'],
            'user_id': row_dict['user_id'],
            'business': b_url,
            'stars': content['stars'],
            'review_text': review_text,
            'self': r_url
        }
         



# Get all reviews for a user
@app.route('/' + USERS + '/<int:user_id>/' + REVIEWS, methods=['GET'])
def get_reviews(user_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM reviews WHERE user_id = :user_id'
        )
        rows = conn.execute(stmt, parameters={'user_id': user_id}).mappings().all()
        reviews = []
        for row in rows:
            row_dict = dict(row)
            b_id = row_dict['business_id']
            b_url = str(request.url_root) + BUSINESSES + '/' + str(b_id)
            del row_dict['business_id']
            row_dict['business'] = b_url
            r_url = str(request.url_root) + REVIEWS + '/' + str(row_dict['id'])
            row_dict['self'] = r_url
            reviews.append(row_dict)
        return reviews



# Delete a review
@app.route('/' + REVIEWS + '/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'DELETE FROM reviews WHERE id=:review_id'
        )
        result = conn.execute(stmt, parameters={'review_id': review_id})
        conn.commit()
        # result.rowcount value will be the number of rows deleted.
        if result.rowcount == 1:
            return ('', 204)
        else:
            return ERROR_REVIEW_NOT_FOUND, 404



if __name__ == '__main__':
    init_db()
    create_table(db)
    app.run(host='0.0.0.0', port=8080, debug=True)