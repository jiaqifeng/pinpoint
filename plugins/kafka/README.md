Since https://github.com/naver/pinpoint/wiki add a link to here, so it's better to explain what's done here.

From view of pinpoint kafka behaviours just like a message queue such as RabbitMq. Then we should:

1 embedded trace info such as transation id into kafka message

2 intercept the method that receive and handle one message

But kafka itself doesn't provide such "high level" things, it provides low level api for efficiency.
So the key to trace kafka is to provide 2 things:

1 formate all kafka messages, such as adding a header to carry trace info

2 locate the method and handle only one kafka message

I write a sample lib at https://github.com/jiaqifeng/quickdemo/tree/master/kafka-message-wrapper to resolve issue 1. Also I add examples for issue 2 in my quickdemo project.

So this is just a referrence solution for trace kafka in real world. Wish it will be helpful.
