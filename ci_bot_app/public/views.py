# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import os
import sys
import json
from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask import abort
from flask_login import login_required, login_user, logout_user

from ci_bot_app.extensions import login_manager, cache
from ci_bot_app.public.forms import LoginForm
from ci_bot_app.user.forms import RegisterForm
from ci_bot_app.user.models import User
from ci_bot_app.utils import flash_errors
import confluent_kafka

blueprint = Blueprint('public', __name__, static_folder='../static')



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

@blueprint.route('/bot/', methods=['POST'])
def bot_receive():
    TOPIC = "{}bot".format(os.environ['CLOUDKARAFKA_TOPIC_PREFIX'])
    conf = {
            'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
            'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD'],
    }

    PRODUCER = confluent_kafka.Producer(**conf)

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
