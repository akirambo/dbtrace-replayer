
require 'mail'
mail = Mail.new do
  from     'task_finished@kuroda.net'
  to       'akuroda@andrew.cmu.edu'
  subject  'Task is finished'
  body     'No Text.'
end
 
mail.delivery_method :sendmail
 
mail.deliver
