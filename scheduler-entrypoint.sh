#!/bin/bash

celery beat --max-interval 300 -l INFO -A gwvolman.scheduler -b redis://redis:6379/  --result-backend redis://redis:6379/
