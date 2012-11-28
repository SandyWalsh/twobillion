compute_events = [
    'compute.instance.finish_resize.*',
    'compute.instance.power_off.*',
    'compute.instance.power_on.*',
    'compute.instance.reboot.*',
    'compute.instance.rebuild.*',
    'compute.instance.resize.confirm.*',
    'compute.instance.resize.prep.*',
    'compute.instance.resize.revert.*',
    'compute.instance.resize.*',
    'compute.instance.shutdown.*',
    'compute.instance.snapshot.*',
    'compute.instance.suspend',
    'compute.instance.resume',
    'compute.instance.exists',
    'compute.instance.update',
    'attach_volume',
    'change_instance_metadata',
    'detach_volume',
    'finish_resize',
    'finish_revert_resize',
    'get_vnc_console',
    'power_on_instance',
    'prep_resize',
    'reboot_instance',
    'rebuild_instance',
    'rescue_instance',
    'reserve_block_device_name',
    'resize_instance',
    'revert_resize',
    'run_instance',
    'set_admin_password',
    'snapshot_instance',
    'start_instance',
    'suspend_instance',
    'terminate_instance',
    'unrescue_instance']

scheduler_events = ['scheduler.run_instance.start',
                    'scheduler.run_instance.scheduled',
                    'scheduler.run_instance.end']

schedulers = ['scheduler_%02d' % x for x in xrange(3)]

compute_nodes = ['compute_%03d' % x for x in xrange(100)]

api_nodes = ['api.server.%02d' % x for x in xrange(10)]
