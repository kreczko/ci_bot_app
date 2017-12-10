# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import os
from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask import abort
from flask_login import login_required, login_user, logout_user

from ci_bot_app.extensions import login_manager, cache
from ci_bot_app.public.forms import LoginForm
from ci_bot_app.user.forms import RegisterForm
from ci_bot_app.user.models import User
from ci_bot_app.utils import flash_errors
import redis
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

blueprint = Blueprint('public', __name__, static_folder='../static')

rclient = redis.from_url(os.environ.get("REDIS_URL"))

TOPIC = "{}.test".format(os.environ['CLOUDKARAFKA_TOPIC_PREFIX'])
    conf = {
        'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
    }

PRODUCER = Producer(**conf)
CONSUMER = Consumer(**conf)
CONSUMER.subscribe([topic])

@login_manager.user_loader
def load_user(user_id):
    """Load user by ID."""
    return User.get_by_id(int(user_id))


@blueprint.route('/', methods=['GET', 'POST'])
def home():
    """Home page."""
    form = LoginForm(request.form)
    # Handle logging in
    if request.method == 'POST':
        if form.validate_on_submit():
            login_user(form.user)
            flash('You are logged in.', 'success')
            redirect_url = request.args.get('next') or url_for('user.members')
            return redirect(redirect_url)
        else:
            flash_errors(form)
    return render_template('public/home.html', form=form)


@blueprint.route('/logout/')
@login_required
def logout():
    """Logout."""
    logout_user()
    flash('You are logged out.', 'info')
    return redirect(url_for('public.home'))


@blueprint.route('/register/', methods=['GET', 'POST'])
def register():
    """Register new user."""
    form = RegisterForm(request.form)
    if form.validate_on_submit():
        User.create(username=form.username.data, email=form.email.data, password=form.password.data, active=True)
        flash('Thank you for registering. You can now log in.', 'success')
        return redirect(url_for('public.home'))
    else:
        flash_errors(form)
    return render_template('public/register.html', form=form)


@blueprint.route('/about/')
def about():
    """About page."""
    form = LoginForm(request.form)
    return render_template('public/about.html', form=form)

@blueprint.route('/bot/', methods=['GET'])
def bot():
    # CI_KEY = 'ci_test'
    # ci_test = rclient.get(CI_KEY)
    # if ci_test:
    #     return str(ci_test)
    # return "No entries"
    msg = CONSUMER.poll(timeout=1.0)
    if msg is None:
        return "No entries"
    if msg.error():
    # Error or event
        if msg.error().code() == KafkaError._PARTITION_EOF:
        # End of partition event
        return ('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            # Error
            raise KafkaException(msg.error())
    else:
        # Proper message
        sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
        return msg.value()

@blueprint.route('/bot/', methods=['POST'])
def bot_receive():
    TOKEN = os.environ.get("GITLAB_TOKEN")

    if TOKEN is not None:
        if request.headers.get('X-Gitlab-Token') != TOKEN:
            abort(401)
            return
        try:
            PRODUCER.produce(TOPIC, request.get_json(silent=True))
        except:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(PRODUCER))
        PRODUCER.poll(0)
        PRODUCER.flush()
    # content = request.get_json(silent=True)
    # js = json.dumps(content)
    # producer.send(TOPIC_PREFIX + 'gitlabjson', js)
    # producer.flush()
    # bs = bson.dumps(content)
    # producer.send(TOPIC_PREFIX + 'gitlabbson', bs)
    # producer.flush()
    return "OK"
