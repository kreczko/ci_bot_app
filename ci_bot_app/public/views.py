# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import os
import sys
from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask import abort
from flask_login import login_required, login_user, logout_user

from ci_bot_app.extensions import login_manager, cache
from ci_bot_app.public.forms import LoginForm
from ci_bot_app.user.forms import RegisterForm
from ci_bot_app.user.models import User
from ci_bot_app.utils import flash_errors
import json
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

blueprint = Blueprint('public', __name__, static_folder='../static')

TOPIC = "{}.test".format(os.environ['CLOUDKARAFKA_TOPIC_PREFIX'])
conf = {
        'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD'],
}

PRODUCER = Producer(**conf)

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

# @blueprint.route('/bot/', methods=['GET'])
# def bot():
#     # CI_KEY = 'ci_test'
#     # ci_test = rclient.get(CI_KEY)
#     # if ci_test:
#     #     return str(ci_test)
#     # return "No entries"
#     msg = CONSUMER.poll(timeout=1.0)
#     if msg is None:
#         return "No entries"
#     if msg.error():
#         if msg.error().code() == KafkaError._PARTITION_EOF:
#             out = '%% {} [{}] reached end at offset {}\n'
#             out = out.format(msg.topic(), msg.partition(), msg.offset())
#             return out
#         elif msg.error():
#             raise KafkaException(msg.error())
#     else:
#         out = '%% {} [{}] at offset {} with key %s:\n'
#         out = out.format(msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
#         sys.stderr.write(out)
#         return msg.value()

@blueprint.route('/bot/', methods=['POST'])
def bot_receive():
    TOKEN = os.environ.get("GITLAB_TOKEN")

    if TOKEN is not None:
        if request.headers.get('X-Gitlab-Token') != TOKEN:
            abort(401)
            return
        content = request.get_json(silent=True)
        js = json.dumps(content)
        # bs = bson.dumps(content)

        PRODUCER.produce(TOPIC, js)
        PRODUCER.flush()

    return "OK"
