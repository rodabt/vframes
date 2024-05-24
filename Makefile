.PHONY: link docs

link:
	ln -s /home/rabt/.vmodules/rodabt/vframes /home/rabt/devel/vframes

docs:
	VDOC_SORT=false v doc -comments -color -f html -m vframes -o docs -readme -inline-assets